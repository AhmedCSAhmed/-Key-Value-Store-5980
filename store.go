package main
import (
	"net/http"
	"errors"
)

var store = make(map[string]any) // persitantce store in memory for now

func main() {	

	server()
	http.ListenAndServe(":8090", nil)

}


func get(key string) (string, error) {
	value, exists := store[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return value, nil
}


func put(key string, value string) {
	store[key] = value
}


func delete(key string) error{
	value, exists := store[key]
	if !exists {
		return errors.New("key not found")
	}
	delete(store, key)
	return nil
}




func server(){

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request){
		resp  := r.URL.Query().Get("key")
		if resp == "" {
			http.Error(w,"key is required and cannot be empty", http.StatusBadRequest)
			return 
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	value, err := get(resp)
	if err  != nil {
		http.Error(w,"internal server error",http.StatusInternalServerError)
		return 
	}

	if value == "" {
		http.Error(w,"key not found",http.StatusNotFound)
		return 
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
})

http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request){
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w,"key and value are required and cannot be empty", http.StatusBadRequest)
		return 
	}
	err := put(key, value)

	if err != nil {
		http.Error(w,"internal server error",http.StatusInternalServerError)
		return 
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("key-value pair added successfully"))
})

http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request){
	key := r.URL.Query().Get("key")
	if key == ""{
		http.Error(w,"key is required and cannot be empty", http.StatusBadRequest)
		return 
	}
	err := delete(key)
	if err != nil {
		http.Error(w,"internal server error",http.StatusInternalServerError)
		return 
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("key-value pair deleted successfully"))
	})
}

