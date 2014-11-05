package s3sync

import (
	"gopkg.in/fsnotify.v1"
	"log"
	"os"
	"path/filepath"
	"time"
)

func (this S3Sync) WatchDir() (results <-chan UploadEvent, err error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	err = this.addAllToWatcher(watcher)
	if err != nil {
		watcher.Close() // Can't defer at creation, because this outer function ends before the go func() below will be finished
		return nil, err
	}
	output := make(chan UploadEvent)

	go func() {
		defer close(output)
		defer watcher.Close()
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if filepath.Base(event.Name) == ".DS_Store" {
						continue
					}
					localFullPath := event.Name
					evt := UploadEvent{RemotePath: "", LocalFileName: localFullPath, StartTime: time.Now()}
					log.Println("Event: ", event)
					output <- evt
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()
	return this.waitTillDoneWriting(output), nil
}

func (this S3Sync) waitTillDoneWriting(incoming <-chan UploadEvent) <-chan UploadEvent {

	/**
	  TODO: I may be able to do this differently depending on how the fsnotify works.  If I keep getting 'WRITE' events, until it stops,
	        I may be able to then just delay that one before sending it to the next in the pipeline.  This way will prevent the need
	        to reopen the file every 5 seconds.  This will also save 1 go func() in the pipeline
	 **/

	output := make(chan UploadEvent)
	go func() {
		defer close(output)
		items := make(map[string]*EventWrapper)
		for {
			select {
			case evt, ok := <-incoming:
				if !ok {
					return
				}
				//must be negative cause it could take a while before the first bytes are written
				//Pointer so that our range below will not get copies of the items on iteration
				items[evt.LocalFileName] = &EventWrapper{evt, -1}
			case <-time.After(time.Second * 5):

			}
			for _, item := range items {
				if !item.HasChanged() {
					log.Println(item.evt.LocalFileName, " has not changed in 5 seconds, time to upload it")
					delete(items, item.evt.LocalFileName)
					output <- item.evt
				}
			}
		}
	}()
	return output
}

func (this S3Sync) addAllToWatcher(watcher *fsnotify.Watcher) (err error) {
	rootLocalDir, _ := filepath.Abs(this.LocalPath)
	walkFn := func(path string, f os.FileInfo, err error) error {
		if filepath.Base(path) == ".ebagmd" {
			// we do not want to ever watch these directories, or anything inside of it
			return filepath.SkipDir
		}
		if f.IsDir() {
			log.Println("Watching path ", path)
			watcher.Add(path)
		}
		return nil
	}
	return filepath.Walk(rootLocalDir, walkFn)
}

type EventWrapper struct {
	evt      UploadEvent
	lastsize int64
}

/**
  This function modifies EventWrapper, by updating lastsize.  Its therefore is not threadsafe
  Returns true if the file has changed since last checked, otherwise false
*/
func (this *EventWrapper) HasChanged() bool {
	file, err := os.Open(this.evt.LocalFileName)
	if err != nil {
		return true
	}
	defer checkClose(file, &err)
	stat, err := file.Stat()
	if err != nil {
		return true
	}
	new_filesize := stat.Size()
	has_changed := (this.lastsize != new_filesize)
	log.Printf("%s old=%d, new=%d, changed=%b", this.evt.LocalFileName, this.lastsize, new_filesize, has_changed)
	this.lastsize = new_filesize
	return has_changed
}
