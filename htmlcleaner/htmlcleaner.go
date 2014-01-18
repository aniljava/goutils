package htmlcleaner

import (
	"code.google.com/p/go-html-transform/h5"
	"code.google.com/p/go.net/html"
	"github.com/aniljava/goutils/generalutils"
	"strings"
)

var DefaultTags []string = []string{
	"p",
	"b",
	"br",
	"table",
	"tr",
	"td",
	"th",
	"div",
}
var DefaultAttrs []string = []string{}

func Clean(data string, tags []string, attributes []string) string {
	if tags == nil {
		tags = DefaultTags
	}
	if attributes == nil {
		attributes = DefaultAttrs
	}

	if tree, err := h5.NewFromString(data); err == nil {
		result := WalkNodes(tree.Top(), tags, attributes)
		result = generalutils.CompactTrim(result)
		result = format(result)
		return result
	}
	return data
}

func format(data string) string {
	return data
}

func WalkNodes(n *html.Node, tags, attributes []string) string {

	pre, post, content := "", "", ""

	if n != nil {
		tag := n.DataAtom.String()
		val := n.Data

		include := false
		for _, inc := range tags {
			include = include || inc == tag
		}
		if include {
			pre = "<" + tag + ">"
			post = "</" + tag + ">"
		}

		if n.Type == 4 || (n.Type == 1 && strings.TrimSpace(val) == "") {
			return ""
		}

		if n.Type == 1 {
			content = generalutils.CompactTrim(n.Data)
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if result := WalkNodes(c, tags, attributes); strings.TrimSpace(result) != "" {
				if !strings.HasPrefix(result, "<") {
					content += " "
				}
				content += result
			}
		}

	}

	if strings.TrimSpace(content) != "" {
		result := pre + content + post
		if pre == "<b>" && len(content) > 64 {
			result = content
		}
	} else {
		return ""
	}
}
