package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	rtesting "github.com/szermatt/realize/internal/testing"
)

func setupLocalRemote(f *rtesting.DirFixture, count int) {

	f.MakeDir("remote/sourcedir")
	f.MakeDir("local")

	for i := 0; i < count; i++ {
		p := fmt.Sprintf("remote/sourcedir/source%d/content%d", i, i)
		f.WriteFile(p, fmt.Sprintf("content%d", i))
		f.SymLink(p, fmt.Sprintf("local/dest%d_dir/dest%d", i, i))
	}
}

func TestCollectTargets(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	setupLocalRemote(f, 3)

	f.MakeDir("wrongremote")
	f.Touch("wrongremote/content")
	f.SymLink("wrongremote/content", "local/wrong")

	targets, err := collectTargets(f.Path("local"), f.Path("remote"))
	assert.Nil(err)
	assert.Equal(
		[]*target{
			&target{
				remotePath: f.Path("remote/sourcedir/source0/content0"),
				localPath:  f.Path("local/dest0_dir/dest0"),
			},
			&target{
				remotePath: f.Path("remote/sourcedir/source1/content1"),
				localPath:  f.Path("local/dest1_dir/dest1"),
			},
			&target{
				remotePath: f.Path("remote/sourcedir/source2/content2"),
				localPath:  f.Path("local/dest2_dir/dest2"),
			},
		}, targets)
}

func TestRealize(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	setupLocalRemote(f, 2)

	err := RunRealize(
		context.Background(), f.Path("local"), f.Path("remote"), Options{})
	assert.Nil(err)
	assert.False(f.IsSymlink("local/dest0_dir/dest0"))
	assert.Equal("content0", f.FileContentString("local/dest0_dir/dest0"))
	assert.False(f.IsSymlink("local/dest1_dir/dest1"))
	assert.Equal("content1", f.FileContentString("local/dest1_dir/dest1"))
}

func TestDeleteOption(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	setupLocalRemote(f, 2)
	f.Touch("remote/sourcedir/source1/force_keep")

	err := RunRealize(
		context.Background(), f.Path("local"), f.Path("remote"), Options{
			Delete: true,
		})
	assert.Nil(err)
	assert.False(f.IsSymlink("local/dest0_dir/dest0"))
	assert.False(f.IsSymlink("local/dest1_dir/dest1"))
	assert.False(f.Exists("remote/sourcedir/source0/content0"))
	assert.False(f.Exists("remote/sourcedir/source1/content1"))

	// dir source0 has been deleted
	assert.False(f.Exists("remote/sourcedir/source0"))

	// dir source1 should have been kept because it's not empty
	assert.True(f.Exists("remote/sourcedir/source1"))
}
