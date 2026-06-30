package gofiber_session

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/session/v2"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type Session struct {
	Test string
}

func (s *Session) GetTest() string {
	return s.Test
}

func SessionRequire(config Config) fiber.Handler {

	return func(ctx *fiber.Ctx) error {

		if config.Session == nil {
			return unAuthorizedHandler(ctx, "SessionRequire-Middleware. Session is nil.")
		}

		store := config.Session.Get(ctx)
		if store == nil {
			return unAuthorizedHandler(ctx, "SessionRequire-Middleware. Store is nil.")
		}

		cookie := store.Get(STORED_COOKIE_NAME)
		if cookie == nil {
			return unAuthorizedHandler(ctx, fmt.Sprintf("SessionRequire-Middleware. Unable to find session for Cookie: %s ", STORED_COOKIE_NAME))
		} else {
			log.Printf("SessionRequire-Middleware. Cookie: %s has been found. So far so good Cookie value:%s", STORED_COOKIE_NAME, cookie)
		}

		if err := ctx.Next(); err != nil {
			return err
		}

		return nil
	}

}

func AuthRequire(config Config) fiber.Handler {
	return func(ctx *fiber.Ctx) error {

		cookieTk := ctx.Cookies(config.CookieName, INVALID_ACCESS)
		//Check cookie to authorize valid call's source
		if cookieTk == INVALID_ACCESS {
			err := ctx.Status(http.StatusUnauthorized).JSON(&fiber.Map{"status": http.StatusBadRequest, "code": http.StatusUnauthorized, "message": "Unauthorized Access"})
			if err != nil {
				return err
			}
			return fmt.Errorf("Unauthorized Access")
		}

		log.Printf("****AuthRequire TRX_CUST_NUM: %s", cookieTk)

		if config.Session == nil {
			return unAuthorizedHandler(ctx, "AuthRequire-Middleware. Session is nil.")
		}

		store := config.Session.Get(ctx)
		if store == nil {
			return unAuthorizedHandler(ctx, "AuthRequire-Middleware. Store is nil.")
		}

		defer func(store *session.Store) {
			err := store.Save()
			if err != nil {
				log.Printf("Error saving store: %v", err)
			}
		}(store)

		onUrl := IdentityObj{}

		q, err := url.ParseQuery(string(ctx.Request().URI().QueryString()))
		if err != nil {
			log.Printf(" ERROR parsing query: %v", err)
			err := ctx.Status(http.StatusBadRequest).JSON(&fiber.Map{"status": http.StatusBadRequest, "code": "Invalid-Query-String", "message": "Invalid Access"})
			if err != nil {
				return err
			}
			return fmt.Errorf("Unauthorized Access")
		}

		//Read URL Querystring
		onUrl.AppId = q.Get("appid")
		onUrl.Mode = q.Get("mode")
		onUrl.TrxISAT = q.Get("TRX-ISAT")
		onUrl.View = q.Get("view")
		onUrl.SSCOMMON = q.Get("SSCOMMON")
		onUrl.ProfileName = q.Get("PROFILENAME")

		log.Print("New Session, verify identity with IdentityService!")

		//Verify identity
		loginUrl := fmt.Sprintf("%s?appid=%s&SSCOMMON=%s&view=%s&PROFILENAME=%s&mode=%s", config.LoginUrl, onUrl.AppId, onUrl.SSCOMMON, onUrl.View, onUrl.ProfileName, onUrl.Mode)

		if len(strings.TrimSpace(onUrl.TrxISAT)) == 0 {
			log.Printf("Redirect loginUrl: %s", loginUrl)
			return ctx.Redirect(loginUrl)
		}

		req, _ := http.NewRequest(http.MethodPost, config.CredentialUrl, bytes.NewBuffer([]byte(onUrl.TrxISAT)))

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error user authentication: %v", err)
			return ctx.Redirect(loginUrl)
		}

		if resp == nil {
			log.Printf("Identity Response is nil")
			return ctx.Redirect(loginUrl)
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				log.Printf("Error closing body: %v", err)
			}
		}(resp.Body)

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			log.Printf("Identity Status Code %v", resp.StatusCode)
			return ctx.Redirect(loginUrl)
		}

		//Read session details data from identity resp
		userSessionDetail := SessionDetails{}
		err = json.NewDecoder(resp.Body).Decode(&userSessionDetail)
		if err != nil {
			log.Printf("Session Details resp error %v", err)
			return ctx.Redirect(loginUrl)
		}

		//Save session
		if len(onUrl.View) > 0 {
			userSessionDetail.AppView = onUrl.View
			store.Set("VIEW", onUrl.View)
			store.Set("AppView", userSessionDetail.AppView)
		}
		store.Set(STORED_COOKIE_NAME, cookieTk)
		store.Set("AccountId", userSessionDetail.AccountId)
		store.Set("FirstName", userSessionDetail.FirstName)
		store.Set("LastName", userSessionDetail.LastName)
		store.Set("DefaultProfile", userSessionDetail.DefaultProfile)
		store.Set("UserId", userSessionDetail.UserId)

		if err := ctx.Next(); err != nil {
			return err
		}

		return nil
	}
}

var openResourceRegexp *regexp.Regexp

// CaptureIDTMiddleware records the proxy-injected TRX_IDT header into the
// in-process IDT registry on every request, keyed by the gofiber session id.
// Reusable across any webapp behind the proxy: app.Use(CaptureIDTMiddleware(sess)).
// No-op when no IDT header is present (IDT off / non-IDT request). Read-only with
// respect to the session — it only reads store.ID(); it never mutates the session.
// Register it AFTER AuthorizationProxyCheck so the session cookie is resolvable.
func CaptureIDTMiddleware(session *session.Session) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		if idt := ReadIDT(ctx); idt != "" && session != nil {
			if store := session.Get(ctx); store != nil {
				CaptureIDT(store.ID(), idt)
			}
		}
		return ctx.Next()
	}
}

func AuthorizationProxyCheck(session *session.Session) fiber.Handler {
	var combinedOpenResourcePatternsEnv = os.Getenv("OPEN_RESOURCE_PATTERNS")
	combinedOpenResourcePatterns := ".*/gxt/.*|.*nocache.*|.*\\.cache\\..*|.*\\/bootstrap\\.min\\..*|angular\\.min\\.js|.*\\/zapatec\\/.*\\..*|.*\\/pdfjs\\/.*\\.js(?:\\?.*)?$|.*\\.(jpg|jpeg|png|gif|svg|woff2|woff|ttf)(?:\\?.*)?$|.*\\.css(?:\\?.*)?$|.*\\.map(?:\\?.*)?$" // .*\.js(?:\?.*)?$
	if len(combinedOpenResourcePatternsEnv) > 0 {
		combinedOpenResourcePatterns = combinedOpenResourcePatternsEnv
	}

	if len(combinedOpenResourcePatterns) > 0 {
		var err error
		if openResourceRegexp, err = regexp.Compile(combinedOpenResourcePatterns); err != nil {
			log.Panicf("Error compiling openResourceRegexp: %v", err)
		}
	}
	return func(ctx *fiber.Ctx) error {
		//log.Println("-------------------------------------------")
		//log.Printf("-> AuthorizationFilter -  %s", ctx.OriginalURL())

		if websocket.IsWebSocketUpgrade(ctx) {
			return ctx.Next()
		}

		q, err := url.ParseQuery(string(ctx.Request().URI().QueryString()))
		if err != nil {
			return unAuthorizedHandler(ctx, fmt.Sprintf("Error parsing query: %v", err))
		}

		path := ctx.Path()
		//CHECK IF URL MATCHES THE OPEN RESOURCE REGEXP. ALLOW ACCESS BECAUSE THESE RESOURCES ARE AUTHORIZED TO BE OPENED DUE TO THEY MUSTILY CAME FROM CLOUDFRONT WITHOUT SESSION
		if openResourceRegexp != nil && openResourceRegexp.MatchString(path) {
			log.Printf("Resource Match to Opened Pattern: %s", ctx.OriginalURL())
			return ctx.Next()
		}

		if session == nil {
			return unAuthorizedHandler(ctx, "AuthorizationProxyCheck-Middleware. Session is nil.")
		}

		store := session.Get(ctx)
		if store == nil {
			return unAuthorizedHandler(ctx, "AuthorizationProxyCheck-Middleware. Store is nil.")
		}
		saveStoreRequired := false

		//VIEW
		if updateSessionFromSources(store, ctx, q, VIEW, TRX_VIEW) {
			saveStoreRequired = true
		}

		userDetailsStoreStr, userDetailsStoreOk := getSessionString(store, TRX_USER_DETAILS)
		userDetailsHeaderStr := getFromHeader(TRX_USER_DETAILS, ctx)

		//if userDetailsStoreStr != nil && len(*userDetailsStoreStr) > 0 {
		//	log.Printf("User Details Store has value.", *userDetailsStoreStr)
		//}

		if !userDetailsStoreOk && (userDetailsHeaderStr == nil || len(*userDetailsHeaderStr) == 0) {
			ctx.Status(http.StatusUnauthorized).JSON(&fiber.Map{"status": http.StatusUnauthorized, "code": "Unauthorized-Access", "message": "Unauthorized Access"})
			return fmt.Errorf("unauthorized Access")
		}

		if userDetailsHeaderStr == nil || len(*userDetailsHeaderStr) == 0 {
			//log.Print("user Details Header is empty and will continue use from store, then next() ")
			return ctx.Next()
		}

		if !userDetailsStoreOk || *userDetailsHeaderStr != userDetailsStoreStr {
			//log.Print("user Details Header !=  user Details Store, then update it on store")
			store.Set(TRX_USER_DETAILS, *userDetailsHeaderStr)
			saveStoreRequired = true
		}

		if saveStoreRequired {
			err := store.Save()
			if err != nil {
				return err
			}
		}
		return ctx.Next()
	}
}

func ConnectionLimiter(maxConnectCount int, expiration time.Duration, skip func(c *fiber.Ctx) bool) fiber.Handler {
	limiterConfig := limiter.Config{
		Max:        maxConnectCount, // 5
		Expiration: expiration,      // 5 * time.Second, // expiration time of the limit
		Next:       skip,
	}

	return limiter.New(limiterConfig)
}

func updateSessionFromSources(store *session.Store, ctx *fiber.Ctx, query url.Values, sessionKey string, headerKey string) bool {
	currentVal, _ := getSessionString(store, sessionKey)

	if headerVal := getFromHeader(headerKey, ctx); hasValue(headerVal) && isDifferent(currentVal, *headerVal) {
		store.Set(sessionKey, *headerVal)
		return true
	}

	queryVal := strings.TrimSpace(query.Get(strings.ToLower(sessionKey)))
	if len(queryVal) > 0 && isDifferent(currentVal, queryVal) {
		store.Set(sessionKey, queryVal)
		return true
	}

	return false
}
