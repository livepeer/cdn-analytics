package utils

import (
	"errors"
	"path"
	"strings"

	"github.com/golang/glog"
)

type (
	IDType string
)

const (
	IDTypeManifestID IDType = "manifest_id"
	IDTypeStreamID   IDType = "stream_id"
	// IDTypeStreamName IDType = "stream_name"
)

var (
	errNotPlaybackURL = errors.New("invalid URL format: first token should be one of hls, recording or live")
	errWrongExtension = errors.New("invalid URL format - url not ending with index.m3u8 or ts or .mp4")
)

func Includes(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func IsCommentLine(line string) bool {
	return strings.HasPrefix(line, "#")
}

func IsEmptyLine(line string) bool {
	return line == ""
}

func GetStreamId(url string) (string, IDType, error) {
	toks := strings.Split(url, "/")
	lenght := len(toks)
	var idType IDType
	if lenght < 4 {
		return "", "", errors.New("invalid URL format")
	}
	ext := path.Ext(toks[lenght-1])

	if !Includes(allowedExts, ext) {
		return "", "", errWrongExtension
	}

	id := toks[2]

	switch toks[1] {
	case "hls", "cmaf":
		idType = IDTypeManifestID
	case "recordings":
		idType = IDTypeStreamID
	case "live":
		idType = IDTypeManifestID
		panic(url)
	default:
		return "", "", errNotPlaybackURL
	}

	if strings.HasPrefix(id, "video+") {
		if toks[3] == "hls" {
			glog.Infof("==> strange url: %q", url)
			panic("strange url ")
			return id[6:], IDTypeManifestID, nil
		}
		// glog.Infof("####> stream name url=%q", url)
		// return toks[2][6:], IDTypeStreamName, nil
		// id = id[6:]
		// return toks[2][6:], IDTypeStreamName, nil
	}
	id = strings.TrimPrefix(id, "video+")
	id = strings.TrimPrefix(id, "videorec+")

	return id, idType, nil
}

var allowedExts = []string{".m3u8", ".ts", ".mp4", ".m4s"}
