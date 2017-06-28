package ds2bq

import (
	"context"

	"github.com/favclip/ucon"
	"google.golang.org/appengine"
)

// UseAppengineContext do DI to ucon.Bubble.
func UseAppengineContext(b *ucon.Bubble) error {
	c := appengine.NewContext(b.R)
	c = context.WithValue(c, ucon.PathParameterKey, b.Context.Value(ucon.PathParameterKey))

	b.Context = c

	return b.Next()
}
