package patchain

// KeyStartsWith returns a query parameter to find an object that starts with an exact string
func KeyStartsWith(v string) QueryParams {
	return QueryParams{
		KeyStartsWith: v,
	}
}
