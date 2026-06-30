package gofiber_session

import (
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/session/v2"
	"log"
	"net/http"
	"strings"
)

func hasValue(s *string) bool {
	return s != nil && len(*s) > 0
}

func isDifferent(existing, newVal string) bool {
	return len(strings.TrimSpace(existing)) == 0 || existing != newVal
}

func getFromStore(key string, store *session.Store) *string {
	if store == nil {
		return nil
	}

	viewStored, ok := store.Get(key).(string)
	if ok && len(viewStored) > 0 {
		return &viewStored
	}
	return nil
}

func getFromHeader(key string, ctx *fiber.Ctx) *string {
	if ctx == nil || ctx.Request() == nil {
		return nil
	}

	viewHeaderBA := ctx.Request().Header.Peek(key)
	if len(viewHeaderBA) > 0 {
		value := string(viewHeaderBA)
		return &value
	}
	return nil
}

func unAuthorizedHandler(ctx *fiber.Ctx, messageLog string) error {
	log.Printf("%s", messageLog)
	ctx.Status(http.StatusUnauthorized).JSON(&fiber.Map{"status": http.StatusForbidden, "code": http.StatusUnauthorized, "message": "Unauthorized Access"})
	err := ctx.SendStatus(http.StatusUnauthorized)
	if err != nil {
		return err
	}
	return fmt.Errorf("Unauthorized Access")
}

func getSessionString(store *session.Store, key string) (string, bool) {
	if store == nil {
		return "", false
	}
	val := store.Get(key)
	str, ok := val.(string)
	if !ok || len(strings.TrimSpace(str)) == 0 {
		return "", false
	}
	return str, true
}
