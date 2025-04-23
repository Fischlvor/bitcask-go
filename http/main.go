package main

import (
	bitcask "bitcask-go"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

var db *bitcask.DB

func init() {
	var err error
	options := bitcask.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-gin")
	//options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic("failed to open db: " + err.Error())
	}
}

func handlePut(c *gin.Context) {
	var kv map[string]string
	if err := c.BindJSON(&kv); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("failed to put kv in db: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func handleGet(c *gin.Context) {
	key := c.Query("key")
	value, err := db.Get([]byte(key))
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		} else {
			log.Printf("failed to get kv in db: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": string(value)})
}

func handleDelete(c *gin.Context) {
	key := c.Query("key")
	if err := db.Delete([]byte(key)); err != nil {
		if err != bitcask.ErrKeyIsEmpty {
			log.Printf("failed to delete key in db: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func handleListKeys(c *gin.Context) {
	keys := db.ListKeys()
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	c.JSON(http.StatusOK, result)
}

func handleStat(c *gin.Context) {
	stat := db.Stat()
	c.JSON(http.StatusOK, stat)
}

func main() {
	r := gin.Default()

	r.POST("/bitcask/put", handlePut)
	r.GET("/bitcask/get", handleGet)
	r.DELETE("/bitcask/delete", handleDelete)
	r.GET("/bitcask/listkeys", handleListKeys)
	r.GET("/bitcask/stat", handleStat)

	if err := r.Run("localhost:8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
