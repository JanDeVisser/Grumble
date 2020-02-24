/*
 * This file is part of Finn.
 *
 * Copyright (c) 2019 Jan de Visser <jan@finiandarcy.com>
 *
 * Finn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Finn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Finn.  If not, see <https://www.gnu.org/licenses/>.
 */

package tools

import (
	"database/sql"
	"github.com/JanDeVisser/grumble"
	"log"
	"net/http"
)

//func uploadSchema(w http.ResponseWriter, r *http.Request, mgr *grumble.EntityManager) {
//	// Parse our multipart form, 10 << 20 specifies a maximum
//	// upload of 10 MB files.
//	err := r.ParseMultipartForm(10 << 20)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//
//	// FormFile returns the first file for the given key `myFile`
//	// it also returns the FileHeader so we can get the Filename,
//	// the Header and the size of the file
//	file, _, err := r.FormFile("schema")
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//	defer func() {
//		_ = file.Close()
//	}()
//
//	// Create a temporary file within our temp-images directory that follows
//	// a particular naming pattern
//	tempFile, err := ioutil.TempFile("", "upload-*.json")
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//	defer func() {
//		_ = tempFile.Close()
//		_ = os.Remove(tempFile.Name())
//	}()
//
//	// read all of the contents of our uploaded file into a
//	// byte array
//	fileBytes, err := ioutil.ReadAll(file)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//
//	// write this byte array to our temporary file
//	_, err = tempFile.Write(fileBytes)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//
//	err = mgr.TX(func(conn *sql.DB) error {
//		return ImportSchemaFile(mgr, tempFile.Name())
//	})
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//	} else {
//		http.Redirect(w, r, "/tools/schema", http.StatusSeeOther)
//	}
//}

func resetSchema(w http.ResponseWriter, mgr *grumble.EntityManager) {
	err := mgr.ResetSchema()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = mgr.TX(func(db *sql.DB) error {
		for _, k := range grumble.Kinds() {
			if e := k.Reconcile(mgr.PostgreSQLAdapter); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func SchemaAPI(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s /api/schema ?%s", r.Method, r.URL.RawQuery)
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	switch r.Method {
	//case http.MethodPost:
	//	uploadSchema(w, r, mgr)
	case http.MethodDelete:
		resetSchema(w, mgr)
		//case http.MethodGet:
		//	jsonText, err := ExportSchema(mgr)
		//	if err != nil {
		//		http.Error(w, err.Error(), http.StatusInternalServerError)
		//		return
		//	}
		//	w.Header().Add("Content-type", "application/json")
		//	if r.Form.Get("download") != "" {
		//		w.Header().Add("Content-Disposition", "attachment; filename=\"finnschema.json\"")
		//	}
		//	_, _ = w.Write(jsonText)
	}
}
