package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStreamId(t *testing.T) {
	assert := assert.New(t)
	streamId := "/wp-admin/index.php"
	id, idType, err := GetStreamId(streamId)
	assert.Errorf(err, "%s should be an invalid stream id", streamId)
	assert.Equal("", id)
	assert.Equal(IDType(""), idType)

	streamId = "/hls/fiolz5txbwy3smsr/0_1/index.m3u8"
	id, idType, err = GetStreamId(streamId)
	assert.NoError(err)
	assert.Equal("fiolz5txbwy3smsr", id)
	assert.Equal(IDTypeManifestID, idType)

	id, idType, err = GetStreamId(`/recordings/db90372d-655f-4118-8dcc-7e02b1557bed/source.mp4`)
	assert.NoError(err)
	assert.Equal("db90372d-655f-4118-8dcc-7e02b1557bed", id)
	assert.Equal(IDTypeStreamID, idType)
}
func TestIsCommentLine(t *testing.T) {
	if IsCommentLine("notacomment") {
		t.Errorf("Line should not be a comment")
	}

	if !IsCommentLine("# this is a comment") {
		t.Errorf("Line should be a comment")
	}
}
func TestIsEmptyLine(t *testing.T) {
	if IsEmptyLine("_") {
		t.Errorf("_ should not be an empty line")
	}

	if !IsEmptyLine("") {
		t.Errorf("invalid empty line")
	}
}

func TestFind(t *testing.T) {
	slice := []string{"this", "is", "a", "test"}
	if !Includes(slice, "test") {
		t.Errorf("Invalid result. the string 'test' is included in the slice ")
	}

	if Includes(slice, "nottest") {
		t.Errorf("Invalid result. the string 'nottest' isn't included in the slice ")
	}
}
