package webbridge

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type stubPresigner struct {
	exists      bool
	existsErr   error
	url         string
	presignErr  error
	lastHeadKey string
	lastKey     string
}

func (s *stubPresigner) ChartExists(_ context.Context, key string) (bool, error) {
	s.lastHeadKey = key
	return s.exists, s.existsErr
}
func (s *stubPresigner) PresignChart(_ context.Context, key string) (string, error) {
	s.lastKey = key
	return s.url, s.presignErr
}

func newChartTestApp(p ChartPresigner) *fiber.App {
	app := fiber.New()
	app.Get("/aichatviewer/chart/:name", chartRedirectHandler(p))
	return app
}

const validName = "2eaa8d72-1111-2222-3333-444455556666.png"

func TestChartRedirect_ExistsRedirects(t *testing.T) {
	p := &stubPresigner{exists: true, url: "https://bucket.s3.amazonaws.com/chart/abc.png?sig=1"}
	resp, err := newChartTestApp(p).Test(httptest.NewRequest("GET", "/aichatviewer/chart/"+validName, nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusFound {
		t.Fatalf("status=%d want 302", resp.StatusCode)
	}
	if resp.Header.Get("Location") != p.url {
		t.Fatalf("Location=%q", resp.Header.Get("Location"))
	}
	if p.lastHeadKey != "chart/"+validName || p.lastKey != "chart/"+validName {
		t.Fatalf("head=%q presign=%q", p.lastHeadKey, p.lastKey)
	}
}

func TestChartRedirect_MissingServesPlaceholder(t *testing.T) {
	p := &stubPresigner{exists: false}
	resp, err := newChartTestApp(p).Test(httptest.NewRequest("GET", "/aichatviewer/chart/"+validName, nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status=%d want 200 (placeholder)", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "image/svg+xml") {
		t.Fatalf("content-type=%q want image/svg+xml", ct)
	}
	if p.lastKey != "" {
		t.Fatalf("PresignChart was called for a missing object (key=%q)", p.lastKey)
	}
}

func TestChartRedirect_RejectsBadNames(t *testing.T) {
	p := &stubPresigner{exists: true, url: "https://nope"}
	bad := []string{"..%2f..%2fuploads%2fsecret.pdf", "", "not-a-uuid.png", "2eaa8d72.txt", "2eaa8d72-1111.png.exe", "---------.png", strings.Repeat("a", 600) + ".png"}
	for _, name := range bad {
		resp, err := newChartTestApp(p).Test(httptest.NewRequest("GET", "/aichatviewer/chart/"+name, nil))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusNotFound {
			t.Fatalf("name=%q status=%d want 404", name, resp.StatusCode)
		}
		if p.lastHeadKey != "" || p.lastKey != "" {
			t.Fatalf("name=%q reached S3 (head=%q presign=%q)", name, p.lastHeadKey, p.lastKey)
		}
	}
}

func TestChartRedirect_ExistsCheckErrorIs404(t *testing.T) {
	p := &stubPresigner{existsErr: errors.New("s3 down")}
	resp, _ := newChartTestApp(p).Test(httptest.NewRequest("GET", "/aichatviewer/chart/"+validName, nil))
	if resp.StatusCode != fiber.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
	if p.lastKey != "" {
		t.Fatalf("presign called despite head error")
	}
}

func TestChartRedirect_PresignErrorIs404(t *testing.T) {
	p := &stubPresigner{exists: true, presignErr: errors.New("presign boom")}
	resp, _ := newChartTestApp(p).Test(httptest.NewRequest("GET", "/aichatviewer/chart/"+validName, nil))
	if resp.StatusCode != fiber.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestChartNameRe_TrailingNewline(t *testing.T) {
	if chartNameRe.MatchString(validName + "\n") {
		t.Fatal("regex must reject a trailing newline")
	}
}
