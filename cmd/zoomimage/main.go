package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/png"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"emperror.dev/emperror"
	"emperror.dev/errors"
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/zsearch/v2/pkg/search"
	"github.com/rs/zerolog"
	"golang.org/x/image/draw"
)

const HEIGHT = 150

var mediaserverRegexp = regexp.MustCompile("^mediaserver:([^/]+)/([^/]+)$")

var cfgfile = flag.String("config", "./zoomimage.toml", "locations of config file")

func main() {
	var err error
	flag.Parse()
	config := LoadConfig(*cfgfile)

	//	HEIGHT := config.CHeight

	var out io.Writer = os.Stdout
	if config.Logfile != "" {
		fp, err := os.OpenFile(config.Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", config.Logfile, err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(config.Loglevel))
	var logger zLogger.ZLogger = &_logger

	elasticConfig := elasticsearch.Config{
		APIKey:    string(config.ElasticSearch.ApiKey),
		Addresses: config.ElasticSearch.Endpoint,

		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// Retry up to 5 attempts
		//
		MaxRetries: 5,

		Logger: &elastictransport.ColorLogger{Output: os.Stdout},
		//		Transport: doer,
	}
	elastic, err := elasticsearch.NewTypedClient(elasticConfig)
	if err != nil {
		logger.Fatal().Err(err)
	}

	var query = &types.Query{
		Bool: &types.BoolQuery{
			Should:             []types.Query{},
			MinimumShouldMatch: 1,
			Filter: []types.Query{
				{
					Term: map[string]types.TermQuery{
						"acl.meta.keyword": {Value: "global/guest"},
					},
				},
				{
					Term: map[string]types.TermQuery{
						"acl.content.keyword": {Value: "global/guest"},
					},
				},
			},
		},
	}
	for key, filters := range config.Filters {
		if !strings.HasSuffix(key, ".keyword") {
			key += ".keyword"
		}
		for _, filter := range filters {
			query.Bool.Should = append(query.Bool.Should, types.Query{
				Term: map[string]types.TermQuery{
					key: {
						Value: filter,
					},
				},
			})
		}
	}

	srch := elastic.Search().
		Index(config.ElasticSearch.Index).
		Query(query).
		Sort("signature.keyword").
		Size(1000).
		TrackTotalHits(true)

	var images = []struct {
		signature string
		img       image.Image
	}{}
	var width int64
	var cHeight = config.CHeight

	var sort = []types.FieldValue{}
	for {
		result, err := srch.SearchAfter(sort...).Do(context.Background())
		if err != nil {
			logger.Fatal().Err(err).Msg("error executing search")
		}
		for _, hit := range result.Hits.Hits {
			sort = hit.Sort
			var data = &search.SourceData{}
			if err := json.Unmarshal(hit.Source_, data); err != nil {
				logger.Fatal().Err(err).Msgf("error unmarshalling data for id %s", *hit.Id_)
			}
			if !data.HasMedia {
				logger.Debug().Msgf("%s: no media", *hit.Id_)
				continue
			}
			if err := func(data *search.SourceData) error {
				for mType, mediaList := range data.Media {
					for _, media := range mediaList {
						matches := mediaserverRegexp.FindStringSubmatch(media.Uri)
						if matches == nil {
							logger.Error().Msgf("invalid url format: %s", media.Uri)
							return errors.New(fmt.Sprintf("invalid url: %s", media.Uri))
						}
						collection := matches[1]
						signature := matches[2]
						logger.Info().Msgf("Loading %s", media.Uri)
						switch mType {
						case "image":
							if media.Mimetype == "image/x-canon-cr2" {
								logger.Warn().Msg("ignoring mime type image/x-canon-cr2")
								return nil
							}
						case "video":
							signature += "$$timeshot$$3"
						case "audio":
							signature += "$$poster"
						case "pdf":
							signature += "$$poster"
						default:
							logger.Warn().Msgf("invalid media type - %s", mType)
							return nil
						}
						msUrl := fmt.Sprintf("%s/%s/%s/resize/autorotate/formatpng/size%d0x%d", config.Mediaserver, collection, signature, HEIGHT, HEIGHT)
						/*
							msUrl, err := ms.GetUrl(collection, signature, function)
							if err != nil {
								return errors.Wrapf(err, "cannot create url for %s/%s/%s", collection, signature, function)
							}
						*/
						logger.Info().Msgf("loading media: %s", msUrl)
						client := http.Client{
							Timeout: 3600 * time.Second,
						}
						resp, err := client.Get(msUrl)
						if err != nil {
							return errors.Wrapf(err, "cannot load url %s", msUrl)
						}

						if resp.StatusCode >= 300 {
							logger.Error().Msgf("cannot get image: %v - %s", resp.StatusCode, resp.Status)
							resp.Body.Close()
							//return errors.New(fmt.Sprintf("cannot get image: %v - %s", resp.StatusCode, resp.Status))
							return nil
						}
						img, _, err := image.Decode(resp.Body)
						resp.Body.Close()
						if err != nil {
							logger.Error().Msgf("cannot decode image %s: %v", msUrl, err)
							return nil
						}
						dst := image.NewRGBA(image.Rect(0, 0, (cHeight*img.Bounds().Max.X)/img.Bounds().Max.Y, cHeight))
						draw.ApproxBiLinear.Scale(dst, dst.Rect, img, img.Bounds(), draw.Over, nil)
						images = append(images, struct {
							signature string
							img       image.Image
						}{signature: fmt.Sprintf("%s", data.Signature), img: dst})
						width += int64(img.Bounds().Dx())
					}
				}
				//		logger.Debug(data)
				return nil
			}(data); err != nil {
				logger.Fatal().Err(err).Msgf("error on data for id %s", *hit.Id_)
			}
		}
		if len(result.Hits.Hits) < 1000 {
			break
		}
	}
	rand.Shuffle(len(images), func(i, j int) { images[i], images[j] = images[j], images[i] })

	intDx := config.Width
	intDy := config.Height
	coll := image.NewRGBA(image.Rectangle{
		Min: image.Point{},
		Max: image.Point{X: intDx, Y: intDy},
	})

	row := 0
	posX := 0
	positions := map[string][]image.Rectangle{}
	for i := 0; i < len(images); i++ {
		posY := row * cHeight
		key := i
		img := images[key]
		//	for key, img := range images {
		logger.Info().Msgf("collage image #%v of %v", key, len(images))
		draw.Copy(coll,
			image.Point{X: posX, Y: posY},
			img.img,
			img.img.Bounds(),
			draw.Over,
			nil)
		if _, ok := positions[img.signature]; !ok {
			positions[img.signature] = []image.Rectangle{}
		}
		positions[img.signature] = append(positions[img.signature], image.Rectangle{
			Min: image.Point{X: posX, Y: posY},
			Max: image.Point{X: posX + img.img.Bounds().Dx(), Y: posY + img.img.Bounds().Dy()},
		})
		posX += img.img.Bounds().Max.X
		if posX > intDx {
			posX = 0
			row++
			// repeat cropped image
			i--
		}
		if (row+1)*cHeight > intDy {
			logger.Info().Msgf("collage %v images of %v", key+1, len(images))
			break
		}
	}
	fp, err := os.Create(filepath.Join(config.ExportPath, "collage.png"))
	if err != nil {
		emperror.Panic(errors.Wrap(err, "cannot create collage file"))
	}
	if err := png.Encode(fp, coll); err != nil {
		fp.Close()
		emperror.Panic(errors.Wrap(err, "cannot encode collage png"))
	}
	fp.Close()

	fp, err = os.Create(filepath.Join(config.ExportPath, "collage.json"))
	if err != nil {
		emperror.Panic(errors.Wrap(err, "cannot create collage json file"))
	}
	jsonW := json.NewEncoder(fp)
	if err := jsonW.Encode(positions); err != nil {
		fp.Close()
		emperror.Panic(errors.Wrap(err, "cannot store json"))
	}
	fp.Close()
	fp, err = os.Create(filepath.Join(config.ExportPath, "collage.jsonl"))
	if err != nil {
		emperror.Panic(errors.Wrap(err, "cannot create collage jsonl file"))
	}
	for signature, rects := range positions {
		jsonBytes, err := json.Marshal(map[string]interface{}{
			"signature": signature,
			"rects":     rects,
		})
		if err != nil {
			fp.Close()
			emperror.Panic(errors.Wrap(err, "cannot store JSONL"))
		}
		jsonBytes = append(jsonBytes, []byte("\n")...)
		if _, err := fp.Write(jsonBytes); err != nil {
			fp.Close()
			emperror.Panic(errors.Wrap(err, "cannot store JSONL"))
		}
	}
	fp.Close()
	fp, err = os.Create(filepath.Join(config.ExportPath, "signatures.txt"))
	if err != nil {
		emperror.Panic(errors.Wrap(err, "cannot create signatures file"))
	}
	for signature, _ := range positions {
		str := signature + "\n"
		if _, err := fp.Write([]byte(str)); err != nil {
			fp.Close()
			emperror.Panic(errors.Wrap(err, "cannot store signatures"))
		}
	}
	fp.Close()

}
