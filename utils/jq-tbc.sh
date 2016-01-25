jq -r ".log.pages[0].renderedContent.text" direct.har | base64 -D > eg.html
jq -r ".log.pages[0].renderedElements[0].content" direct.har | base64 -D > eg.png
