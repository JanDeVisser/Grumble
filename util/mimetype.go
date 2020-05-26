/*
 * This file is part of Finn.
 *
 * Copyright (c) 2020 Jan de Visser <jan@finiandarcy.com>
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

package util

import (
	"fmt"
	"path"
	"strings"
)

var mimetypesByExtension map[string]string
var extensionByMimeType map[string]string

func MimeTypeForFile(f string) string {
	return MimeTypeForExtension(path.Ext(f))
}

func MimeTypeForExtension(ext string) (ret string) {
	var ok bool
	if ret, ok = mimetypesByExtension[ext]; !ok {
		ret = "application/octet-stream"
	}
	return
}

func ExtensionsForMimetype(typ string) (ret string) {
	var ok bool
	if ret, ok = extensionByMimeType[typ]; !ok {
		ret = ".bin"
	}
	return
}

func PrimaryExtensionForMimetype(typ string) string {
	return strings.Split(ExtensionsForMimetype(typ), ",")[0]
}

func init() {
	mimetypesByExtension = make(map[string]string)
	extensionByMimeType = make(map[string]string)

	mimetypesByExtension[".aac"] = "audio/aac"
	mimetypesByExtension[".abw"] = "application/x-abiword"
	mimetypesByExtension[".arc"] = "application/x-freearc"
	mimetypesByExtension[".avi"] = "video/x-msvideo"
	mimetypesByExtension[".azw"] = "application/vnd.amazon.ebook"
	mimetypesByExtension[".bin"] = "application/octet-stream"
	mimetypesByExtension[".bmp"] = "image/bmp"
	mimetypesByExtension[".bz"] = "application/x-bzip"
	mimetypesByExtension[".bz2"] = "application/x-bzip2"
	mimetypesByExtension[".csh"] = "application/x-csh"
	mimetypesByExtension[".css"] = "text/css"
	mimetypesByExtension[".csv"] = "text/csv"
	mimetypesByExtension[".doc"] = "application/msword"
	mimetypesByExtension[".docx"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	mimetypesByExtension[".eot"] = "application/vnd.ms-fontobject"
	mimetypesByExtension[".epub"] = "application/epub+zip"
	mimetypesByExtension[".gz"] = "application/gzip"
	mimetypesByExtension[".gif"] = "image/gif"
	mimetypesByExtension[".htm"] = "text/html"
	mimetypesByExtension[".html"] = "text/html"
	mimetypesByExtension[".ico"] = "image/vnd.microsoft.icon"
	mimetypesByExtension[".ics"] = "text/calendar"
	mimetypesByExtension[".jar"] = "application/java-archive"
	mimetypesByExtension[".jpeg"] = "image/jpeg"
	mimetypesByExtension[".jpg"] = "image/jpeg"
	mimetypesByExtension[".js"] = "text/javascript"
	mimetypesByExtension[".json"] = "application/json"
	mimetypesByExtension[".jsonld"] = "application/ld+json"
	mimetypesByExtension[".mid"] = "audio/x-midi"
	mimetypesByExtension[".midi"] = "audio/x-midi"
	mimetypesByExtension[".mjs"] = "text/javascript"
	mimetypesByExtension[".mp3"] = "audio/mpeg"
	mimetypesByExtension[".mpeg"] = "video/mpeg"
	mimetypesByExtension[".mpkg"] = "application/vnd.apple.installer+xml"
	mimetypesByExtension[".odp"] = "application/vnd.oasis.opendocument.presentation"
	mimetypesByExtension[".ods"] = "application/vnd.oasis.opendocument.spreadsheet"
	mimetypesByExtension[".odt"] = "application/vnd.oasis.opendocument.text"
	mimetypesByExtension[".oga"] = "audio/ogg"
	mimetypesByExtension[".ogv"] = "video/ogg"
	mimetypesByExtension[".ogx"] = "application/ogg"
	mimetypesByExtension[".opus"] = "audio/opus"
	mimetypesByExtension[".otf"] = "font/otf"
	mimetypesByExtension[".png"] = "image/png"
	mimetypesByExtension[".pdf"] = "application/pdf"
	mimetypesByExtension[".php"] = "application/php"
	mimetypesByExtension[".ppt"] = "application/vnd.ms-powerpoint"
	mimetypesByExtension[".pptx"] = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	mimetypesByExtension[".rar"] = "application/vnd.rar"
	mimetypesByExtension[".rtf"] = "application/rtf"
	mimetypesByExtension[".sh"] = "application/x-sh"
	mimetypesByExtension[".svg"] = "image/svg+xml"
	mimetypesByExtension[".swf"] = "application/x-shockwave-flash"
	mimetypesByExtension[".tar"] = "application/x-tar"
	mimetypesByExtension[".tif"] = "image/tiff"
	mimetypesByExtension[".tiff"] = "image/tiff"
	mimetypesByExtension[".ts"] = "video/mp2t"
	mimetypesByExtension[".ttf"] = "font/ttf"
	mimetypesByExtension[".txt"] = "text/plain"
	mimetypesByExtension[".vsd"] = "application/vnd.visio"
	mimetypesByExtension[".wav"] = "audio/wav"
	mimetypesByExtension[".weba"] = "audio/webm"
	mimetypesByExtension[".webm"] = "video/webm"
	mimetypesByExtension[".webp"] = "image/webp"
	mimetypesByExtension[".woff"] = "font/woff"
	mimetypesByExtension[".woff2"] = "font/woff2"
	mimetypesByExtension[".xhtml"] = "application/xhtml+xml"
	mimetypesByExtension[".xls"] = "application/vnd.ms-excel"
	mimetypesByExtension[".xlsx"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	mimetypesByExtension[".xml"] = "text/xml"
	mimetypesByExtension[".xul"] = "application/vnd.mozilla.xul+xml"
	mimetypesByExtension[".zip"] = "application/zip"
	mimetypesByExtension[".3gp"] = "audio/3gpp"
	mimetypesByExtension[".3g2"] = "audio/3gpp2"
	mimetypesByExtension[".7z"] = "application/x-7z-compressed"

	for ext, typ := range mimetypesByExtension {
		if cur, ok := extensionByMimeType[typ]; ok {
			extensionByMimeType[typ] = fmt.Sprintf("%s,%s", cur, ext)
		} else {
			extensionByMimeType[typ] = ext
		}
	}
}
