package nats_service_client

const _EMPTY_ = ""

type Header map[string][]string

// Add adds the key, value pair to the header. It is case-sensitive
// and appends to any existing values associated with key.
func (h Header) Add(key, value string) {
	h[key] = append(h[key], value)
}

// Set sets the header entries associated with key to the single
// element value. It is case-sensitive and replaces any existing
// values associated with key.
func (h Header) Set(key, value string) {
	h[key] = []string{value}
}

// Get gets the first value associated with the given key.
// It is case-sensitive.
func (h Header) Get(key string) string {
	if h == nil {
		return _EMPTY_
	}
	if v := h[key]; v != nil {
		return v[0]
	}
	return _EMPTY_
}

// Values returns all values associated with the given key.
// It is case-sensitive.
func (h Header) Values(key string) []string {
	return h[key]
}

// Del deletes the values associated with a key.
// It is case-sensitive.
func (h Header) Del(key string) {
	delete(h, key)
}
