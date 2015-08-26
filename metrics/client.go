package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// All the methods should accept optional tenant..

// TODO: Add statistics support? Error metrics (connection errors, request errors), request metrics:
// totals, request rate? mean time, avg time, max, min, percentiles etc.. ? And clear metrics..
// Stateful metrics? Kinda like Counters.Inc(..) ?

// More detailed error

type HawkularClientError struct {
	msg  string
	Code int
}

func (self *HawkularClientError) Error() string {
	return fmt.Sprintf("Hawkular returned status code %d, error message: %s", self.Code, self.msg)
}

// Client creation and instance config

const (
	base_url string = "hawkular/metrics"
)

type Parameters struct {
	Tenant string // Technically optional, but requires setting Tenant() option everytime
	Host   string
	Path   string // Optional
}

type Client struct {
	Tenant string
	url    *url.URL
	client *http.Client
}

type HawkularClient interface {
	Send(*http.Request) (*http.Response, error)
}

type HawkularClientFunc func(*http.Request) (*http.Response, error)

// The point of this?
func (h HawkularClientFunc) Send(r *http.Request) (*http.Response, error) {
	return h(r)
}

type Options func(HawkularClient) HawkularClient

// Override function to replace the Tenant (defaults to Client default)
func Tenant(tenant string) Options {
	return func(h HawkularClient) HawkularClient {
		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
			r.Header.Set("Hawkular-Tenant", tenant)
			return h.Send(r)
		})
	}
}

// Add payload to the request
func Data(data interface{}) Options {
	return func(h HawkularClient) HawkularClient {
		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
			jsonb, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}

			b := bytes.NewBuffer(jsonb)
			rc := ioutil.NopCloser(b)
			r.Body = rc

			return h.Send(r)
		})
	}
}

// type Request func(r *http.Request)

// func Command(req Request) Options {
// 	return func(h HawkularClient) HawkularClient {
// 		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
// 			// Commands take input Commands which set the URL!
// 			req(r)
// 			return h.Send(r)
// 		})
// 	}
// }

// Create new Definition
func (self *Client) CreateDefinition(md MetricDefinition, o ...Options) (bool, error) {
	c := func(h HawkularClient) HawkularClient {
		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
			r.URL = self.metricsUrl(md.Type)
			r.Method = "POST"
			return h.Send(r)
		})
	}

	// The c must be the first function, not the Options!
	// No prepend?
	o = append(o, c, Data(md))

	_, _ = self.Send(o...)

	return false, nil
}

type Filter func(r *http.Request)

func Filters(f ...Filter) Options {
	return func(h HawkularClient) HawkularClient {
		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
			for _, filter := range f {
				filter(r)
			}
			return h.Send(r)
		})
	}
}

// Fetch definitions, requires HWKMETRICS-232
func (self *Client) GetDefinitions(o ...Options) []MetricDefinition {
	c := func(h HawkularClient) HawkularClient {
		return HawkularClientFunc(func(r *http.Request) (*http.Response, error) {
			r.Method = "GET"
			r.URL = self.metricsUrl(Generic)
			return h.Send(r)
		})
	}

	o = append(o, c)

	_, _ = self.Send(o...)
	// b, err := c.Send(c, o)
	// json unmarshal to MetricDefinitions and return
	return nil
}

func TypeFilter(t MetricType) Filter {
	return func(r *http.Request) {
		// Add type=? to the query
	}
}

func TagsFilter(t map[string]string) Filter {
	return func(r *http.Request) {
		// Add tags=? to the query
		tags := make([]string, 0, len(t))
		for k, v := range t {
			tags = append(tags, fmt.Sprintf("%s:%s", k, v))
		}
		_ = strings.Join(tags, ",") // Add this
	}
}

// The SEND method..

func (self *Client) createRequest() *http.Request {
	req := &http.Request{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       self.url.Host,
	}
	req.Header.Add("Content-Type", "application/json")
	return req
}

func (self *Client) Send(o ...Options) ([]byte, error) {
	// Initialize
	r := self.createRequest()

	// This needs more work
	// for _, f := range o {
	// 	f(r)
	// }

	// if body != nil {
	// 	switch v := body.(type) {
	// 	case *bytes.Buffer:
	// 		req.ContentLength = int64(v.Len())
	// 	case *bytes.Reader:
	// 		req.ContentLength = int64(v.Len())
	// 	case *strings.Reader:
	// 		req.ContentLength = int64(v.Len())
	// 	}
	// }
	return nil, nil
}

/*
c.CreateDefinition(Type(Gauge)) // Nope.. can't be a filter & option at the same time.. blaah
c.CreateDefinition(Type(Gauge), Data(md)) // Why not just gauge, md? Right.. Tenants..

What if all of these take ... ?

c.Send(Tenant("projectId"),
	Command(Definitions(Gauge))) // Command would add URL?

c.Send(Command(CreateDefinition(Gauge)),
       Data(md))

c.Send(Command(GetDefinitions(Type(Gauge), Tags(t)))) // How to set return type? Do I still need those for certain?

c.Definitions(Tenant("projectId"), Type(Gauge), Tags(t)) ? // Is this possible? No

c.Send(Tenant("projectId"),
       Command(Create()),
       Data(md))
*/

/*
c.Get(Tenant("projectId"),
      FetchCommand(func(req, res))) ? // No, I'll need to separate the runs.. ugh. Well, actually.. I'll need the metadata from the query itself. Do I need a struct that contains the info?
*/

// TODO Instrumentation? To get statistics?
// TODO Authorization / Authentication ?

func NewHawkularClient(p Parameters) (*Client, error) {
	if p.Path == "" {
		p.Path = base_url
	}

	u := &url.URL{
		Host:   p.Host,
		Path:   p.Path,
		Scheme: "http",
		Opaque: fmt.Sprintf("//%s/%s", p.Host, p.Path),
	}
	return &Client{
		url:    u,
		Tenant: p.Tenant,
		client: &http.Client{},
	}, nil
}

// Public functions

// Creates a new metric, and returns true if creation succeeded, false if not (metric was already created).
// err is returned only in case of another error than 'metric already created'
func (self *Client) Create(md MetricDefinition) (bool, error) {
	_, err := self.process(self.metricsUrl(md.Type), "POST", md)
	if err != nil {
		if err, ok := err.(*HawkularClientError); ok {
			if err.Code != http.StatusConflict {
				return false, err
			} else {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil

}

// Fetch metric definitions for one metric type
func (self *Client) Definitions(t MetricType) ([]*MetricDefinition, error) {
	q := make(map[string]string)
	q["type"] = t.shortForm()
	url := self.paramUrl(self.metricsUrl(Generic), q)

	b, err := self.process(url, "GET", nil)
	if err != nil {
		return nil, err
	}

	md := []*MetricDefinition{}
	if b != nil {
		if err = json.Unmarshal(b, &md); err != nil {
			return nil, err
		}
	}

	for _, m := range md {
		m.Type = t
	}

	return md, nil
}

// Return a single definition
func (self *Client) Definition(t MetricType, id string) (*MetricDefinition, error) {
	url := self.singleMetricsUrl(t, id)

	b, err := self.process(url, "GET", nil)
	if err != nil {
		return nil, err
	}

	md := MetricDefinition{}
	if b != nil {
		if err = json.Unmarshal(b, &md); err != nil {
			return nil, err
		}
	}
	md.Type = t
	return &md, nil
}

// Tags methods should return tenant also

// Fetch metric definition tags
func (self *Client) Tags(t MetricType, id string) (*map[string]string, error) {
	id_url := self.cleanId(id)
	b, err := self.process(self.tagsUrl(t, id_url), "GET", nil)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string)
	// Repetive code.. clean up with other queries to somewhere..
	if b != nil {
		if err = json.Unmarshal(b, &tags); err != nil {
			return nil, err
		}
	}

	return &tags, nil
}

// Replace metric definition tags
// TODO: Should this be "ReplaceTags" etc?
func (self *Client) UpdateTags(t MetricType, id string, tags map[string]string) error {
	id_url := self.cleanId(id)
	_, err := self.process(self.tagsUrl(t, id_url), "PUT", tags)
	return err
}

// Delete given tags from the definition
func (self *Client) DeleteTags(t MetricType, id_str string, deleted map[string]string) error {
	id := self.cleanId(id_str)
	tags := make([]string, 0, len(deleted))
	for k, v := range deleted {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	j := strings.Join(tags, ",")
	u := self.tagsUrl(t, id)
	self.addToUrl(u, j)
	_, err := self.process(u, "DELETE", nil)
	return err
}

// Take input of single Metric instance. If Timestamp is not defined, use current time
func (self *Client) PushSingleGaugeMetric(id string, m Datapoint) error {
	// id = self.cleanId(id)

	if _, ok := m.Value.(float64); !ok {
		f, err := ConvertToFloat64(m.Value)
		if err != nil {
			return err
		}
		m.Value = f
	}

	if m.Timestamp == 0 {
		m.Timestamp = UnixMilli(time.Now())
	}

	mH := MetricHeader{
		Id:   id,
		Data: []Datapoint{m},
		Type: Gauge,
	}
	return self.Write([]MetricHeader{mH})
}

// Read single Gauge metric's datapoints.
// TODO: Remove and replace with better Read properties? Perhaps with iterators?
func (self *Client) SingleGaugeMetric(id string, options map[string]string) ([]*Datapoint, error) {
	id = self.cleanId(id)
	url := self.paramUrl(self.dataUrl(self.singleMetricsUrl(Gauge, id)), options)

	b, err := self.process(url, "GET", nil)
	if err != nil {
		return nil, err
	}
	metrics := []*Datapoint{}

	if b != nil {
		if err = json.Unmarshal(b, &metrics); err != nil {
			return nil, err
		}
	}

	return metrics, nil

}

// func (self *Client) QueryGaugesWithTags(id string, tags map[string]string) ([]MetricDefinition, error) {
func (self *Client) Write(metrics []MetricHeader, o ...Options) error {
	if len(metrics) > 0 {
		// Should be sorted and splitted by type & tenant..
		metricType := metrics[0].Type // Temp solution
		if err := metricType.validate(); err != nil {
			return err
		}

		// i := self.Send(Command(Write(metricType)), Data(metrics), o)
		// if i.([]MetricHeader) .. etc..

		// This will be buggy, we're sending []metrics.MetricHeader to the tenant() function..
		_, err := self.process(self.dataUrl(self.metricsUrl(metricType)), "POST", metrics)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Client) sortMetrics(metrics []MetricHeader) (map[*SortKey][]MetricHeader, error) {
	// First-key = tenant-type ?
	// Second key the MetricHeaders..

	m := make(map[*SortKey][]MetricHeader)

	// Create map..
	for _, v := range metrics {
		s := &SortKey{Tenant: v.Tenant, Type: v.Type}
		if m[s] == nil {
			m[s] = make([]MetricHeader, 0)
		}
		m[s] = append(m[s], v)
	}

	return m, nil
}

// HTTP Helper functions

func (self *Client) cleanId(id string) string {
	return url.QueryEscape(id)
}

// Override default http.NewRequest to avoid url.Parse which has a bug (removes valid %2F)
func (self *Client) newRequest(url *url.URL, method string, body io.Reader) (*http.Request, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = ioutil.NopCloser(body)
	}

	req := &http.Request{
		Method:     method,
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       rc,
		Host:       url.Host,
	}

	if body != nil {
		switch v := body.(type) {
		case *bytes.Buffer:
			req.ContentLength = int64(v.Len())
		case *bytes.Reader:
			req.ContentLength = int64(v.Len())
		case *strings.Reader:
			req.ContentLength = int64(v.Len())
		}
	}
	return req, nil
}

// Helper function that transforms struct to json and fetches the correct tenant information
// TODO: Try the decorator pattern to replace all these simple functions?
func (self *Client) process(url *url.URL, method string, data interface{}) ([]byte, error) {
	jsonb, err := json.Marshal(&data)
	if err != nil {
		return nil, err
	}
	return self.send(url, method, jsonb, self.tenant(data))
}

func (self *Client) send(url *url.URL, method string, json []byte, tenant string) ([]byte, error) {
	// Have to replicate http.NewRequest here to avoid calling of url.Parse,
	// which has a bug when it comes to encoded url
	req, _ := self.newRequest(url, method, bytes.NewBuffer(json))
	req.Header.Add("Content-Type", "application/json")
	if len(tenant) > 0 {
		req.Header.Add("Hawkular-Tenant", tenant)
	} else {
		req.Header.Add("Hawkular-Tenant", self.Tenant)
	}
	resp, err := self.client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		return b, err
	} else if resp.StatusCode > 399 {
		return nil, self.parseErrorResponse(resp)
	} else {
		return nil, nil // Nothing to answer..
	}
}

func (self *Client) parseErrorResponse(resp *http.Response) error {
	// Parse error messages here correctly..
	reply, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &HawkularClientError{Code: resp.StatusCode,
			msg: fmt.Sprintf("Reply could not be read: %s", err.Error()),
		}
	}

	details := &HawkularError{}

	err = json.Unmarshal(reply, details)
	if err != nil {
		return &HawkularClientError{Code: resp.StatusCode,
			msg: fmt.Sprintf("Reply could not be parsed: %s", err.Error()),
		}
	}

	return &HawkularClientError{Code: resp.StatusCode,
		msg: details.ErrorMsg,
	}
}

// URL functions (...)

func (self *Client) metricsUrl(metricType MetricType) *url.URL {
	mu := *self.url
	self.addToUrl(&mu, metricType.String())
	return &mu
}

func (self *Client) singleMetricsUrl(metricType MetricType, id string) *url.URL {
	mu := self.metricsUrl(metricType)
	self.addToUrl(mu, id)
	return mu
}

func (self *Client) tagsUrl(mt MetricType, id string) *url.URL {
	mu := self.singleMetricsUrl(mt, id)
	self.addToUrl(mu, "tags")
	return mu
}

func (self *Client) dataUrl(url *url.URL) *url.URL {
	self.addToUrl(url, "data")
	return url
}

func (self *Client) addToUrl(u *url.URL, s string) *url.URL {
	u.Opaque = fmt.Sprintf("%s/%s", u.Opaque, s)
	return u
}

func (self *Client) paramUrl(u *url.URL, options map[string]string) *url.URL {
	q := u.Query()
	for k, v := range options {
		q.Set(k, v)
	}

	u.RawQuery = q.Encode()
	return u
}

// If struct has Tenant defined, return it otherwise return self.Tenant
func (self *Client) tenant(i interface{}) string {
	r := reflect.ValueOf(i)

	if r.Kind() == reflect.Ptr {
		r = r.Elem()
	}

	if r.Kind() == reflect.Slice {
		r = r.Index(0)
	}

	if r.Kind() == reflect.Struct {
		v := r.FieldByName("Tenant")
		if v.Kind() == reflect.String && len(v.String()) > 0 {
			return v.String()
		}
	}
	return self.Tenant
}
