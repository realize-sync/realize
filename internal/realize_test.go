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
	f.Touch("remote/sourcedir/source1/leftover")

	err := RunRealize(
		context.Background(), f.Path("local"), f.Path("remote"), Options{
			Delete: true,
		})
	assert.Nil(err)
	assert.False(f.IsSymlink("local/dest0_dir/dest0"))
	assert.False(f.IsSymlink("local/dest1_dir/dest1"))
	assert.False(f.Exists("remote/sourcedir/source0/content0"))
	assert.False(f.Exists("remote/sourcedir/source1/content1"))

	// dirs should not have been deleted
	assert.True(f.Exists("remote/sourcedir/source0"))
	assert.True(f.Exists("remote/sourcedir/source1"))
}

func TestDeleteDirOption(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	setupLocalRemote(f, 2)
	f.Touch("remote/sourcedir/source1/leftover")

	err := RunRealize(
		context.Background(), f.Path("local"), f.Path("remote"), Options{
			Delete:               true,
			DeleteDirAndLeftover: true,
		})
	assert.Nil(err)
	assert.False(f.IsSymlink("local/dest0_dir/dest0"))
	assert.False(f.IsSymlink("local/dest1_dir/dest1"))
	assert.False(f.Exists("remote/sourcedir/source0/content0"))
	assert.False(f.Exists("remote/sourcedir/source1/content1"))

	// dirs should have been deleted
	assert.False(f.Exists("remote/sourcedir/source0"))
	assert.False(f.Exists("remote/sourcedir/source1"))
}

func TestDeleteDirOptionWithError(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	f.MakeDir("remote/sourcedir")
	f.MakeDir("local")
	f.WriteFile("remote/sourcedir/source0/content0", "content")
	f.WriteFile("remote/sourcedir/source0/content1", "content")
	f.SymLink("remote/sourcedir/source0/content0", "local/dest0")
	f.SymLink("remote/sourcedir/source0/content1", "local/dest1")
	// content2 doesn't exist
	f.SymLink("remote/sourcedir/source0/content2", "local/dest2")

	err := RunRealize(
		context.Background(), f.Path("local"), f.Path("remote"), Options{
			Delete:               true,
			DeleteDirAndLeftover: true,
		})
	assert.NotNil(err)
	assert.Equal("Failed to realize 1/3 files.", err.Error())
	assert.False(f.IsSymlink("local/dest0"))
	assert.False(f.IsSymlink("local/dest1"))
	assert.True(f.IsSymlink("local/dest2"))

	assert.True(f.Exists("remote/sourcedir"))
}

func TestTimeout(t *testing.T) {
	assert := assert.New(t)

	f := rtesting.SetupDir()
	defer f.TearDown()

	setupLocalRemote(f, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	err := RunRealize(ctx, f.Path("local"), f.Path("remote"), Options{})
	assert.Equal("Timed out or cancelled. Realized 0 files.", err.Error())
}
