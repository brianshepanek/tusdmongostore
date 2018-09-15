package mongostore

import(
	"io"
	"io/ioutil"
	"bytes"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/brianshepanek/tusd"
)

type MongoStore struct {
	sess *mgo.Session
    db string
    collection string
}

type FileInfo struct {
	Id bson.ObjectId `bson:"_id"`
	Size int64  `bson:"size"`
	SizeIsDeferred bool `bson:"size_is_deferred"`
	Offset int64 `bson:"offset"`
	MetaData map[string]string `bson:"metadata"`
	IsPartial bool `bson:"is_partial"`
	IsFinal bool `bson:"is_final"`
	PartialUploads []string `bson:"partial_uploads"`
}

func New(sess *mgo.Session, db string, collection string) MongoStore {
	return MongoStore{
		sess : sess,
        db : db,
        collection : collection,
	}
}


func (store MongoStore) UseIn(composer *tusd.StoreComposer) {
	composer.UseCore(store)
	composer.UseGetReader(store)
	composer.UseTerminater(store)
}

func (store MongoStore) NewUpload(info tusd.FileInfo) (string, error) {

	//ID
	var err error
	id := bson.NewObjectId()

	//Mongo Info
	req := FileInfo{
		Id : id,
		Size : info.Size,
		SizeIsDeferred : info.SizeIsDeferred,
		Offset : info.Offset,
		MetaData : info.MetaData,
		IsPartial : info.IsPartial,
		IsFinal : info.IsFinal,
		PartialUploads : info.PartialUploads,
	}

	//Clone
    sess := store.sess.Clone()
    defer sess.Close()
    coll := sess.DB(store.db).C(store.collection)
	fs := sess.DB(store.db).GridFS("fs")

	//Insert
    err = coll.Insert(req)
	if err != nil {
		return "", err
	}

	//Grid FS
	file, err := fs.Create(id.Hex())
	if err != nil {
		return "", err
	}

	//Close
	err = file.Close()
	if err != nil {
		return "", err
	}


	return id.Hex(), err
}

func (store MongoStore) WriteChunk(id string, offset int64, src io.Reader) (int64, error) {

	var err error
	var resp FileInfo

	//Clone
    sess := store.sess.Clone()
    defer sess.Close()
    coll := sess.DB(store.db).C(store.collection)
	fs := sess.DB(store.db).GridFS("fs")

	//GridFS
	currentFile, err := fs.Open(id)
	if err != nil {
		return 0, err
	}

	//Current
	var current []byte
	if currentFile.Size() > 0 {
		current, err = ioutil.ReadAll(currentFile)
		if err != nil {
			return 0, err
		}
	}

	//New
	new, err := ioutil.ReadAll(src)
	if err != nil {
		return 0, err
	}

	//Close
	err = currentFile.Close()
	if err != nil {
		return 0, err
	}

	//Remove
	err = fs.Remove(id)
	if err != nil {
		return 0, err
	}

	//Grid FS
	file, err := fs.Create(id)
	if err != nil {
		return 0, err
	}

	//Write
	_, err = file.Write(append(current, new...))
	if err != nil {
		return 0, err
	}

	//Close
	err = file.Close()
	if err != nil {
		return 0, err
	}

	//Find
	err = coll.FindId(bson.ObjectIdHex(id)).One(&resp)
	if err != nil {
		return 0, err
	}

	//Update
	update := map[string]interface{}{
		"$set" : map[string]interface{}{
			"offset" : int64(len(new)) + offset,
		},
	}
	err = coll.UpdateId(bson.ObjectIdHex(id), update)
	return int64(len(new)), err
}

func (store MongoStore) GetInfo(id string) (tusd.FileInfo, error) {

	var resp FileInfo
	var info tusd.FileInfo
	var err error

	//Clone
    sess := store.sess.Clone()
    defer sess.Close()
    coll := sess.DB(store.db).C(store.collection)

	//Find
	err = coll.FindId(bson.ObjectIdHex(id)).One(&resp)
	if err != nil {
		return info, err
	}

	//Set
	info.Size = resp.Size
	info.SizeIsDeferred = resp.SizeIsDeferred
	info.Offset = resp.Offset
	info.MetaData = resp.MetaData
	info.IsPartial = resp.IsPartial
	info.IsFinal = resp.IsFinal
	info.PartialUploads = resp.PartialUploads

	return info, err
}

func (store MongoStore) GetReader(id string) (io.Reader, error) {

	var reader io.Reader
	var err error

	//Clone
    sess := store.sess.Clone()
    defer sess.Close()
    fs := sess.DB(store.db).GridFS("fs")

	//GridFS
	file, err := fs.Open(id)
	if err != nil {
		return reader, err
	}

	//Current
	var body []byte
	if file.Size() > 0 {
		body, err = ioutil.ReadAll(file)
		if err != nil {
			return reader, err
		}
	}

	//Close
	err = file.Close()
	if err != nil {
		return reader, err
	}

	return bytes.NewReader(body), err
}

func (store MongoStore) Terminate(id string) error {

	//Var
	var err error

	//Clone
    sess := store.sess.Clone()
    defer sess.Close()
	coll := sess.DB(store.db).C(store.collection)
    fs := sess.DB(store.db).GridFS("fs")

	//Remove
	err = coll.RemoveId(bson.ObjectIdHex(id))
	if err != nil {
		return err
	}

	//Remove
	err = fs.Remove(id)
	if err != nil {
		return err
	}
	return nil
}
