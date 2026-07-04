package main

type SchemaJson []Entry

type Entry struct {
	Name    string `json:"name"`
	HtmlUrl string `json:"html_url"`
}
