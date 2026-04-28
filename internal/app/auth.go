package app

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const accessCookieName = "llm_gateway_access"

func (a *App) withAccess(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.authorized(r) {
			next(w, r)
			return
		}
		if token := r.URL.Query().Get("token"); token != "" && a.tokenEqual(token) && r.Method == http.MethodGet {
			a.setAccessCookie(w, r, token)
			redirectURL := *r.URL
			q := redirectURL.Query()
			q.Del("token")
			redirectURL.RawQuery = q.Encode()
			http.Redirect(w, r, redirectURL.String(), http.StatusFound)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="llm-api-gateway"`)
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid gateway access token")
	}
}

func (a *App) authorized(r *http.Request) bool {
	if a.cfg.AccessToken == "" {
		return true
	}
	if token := bearerToken(r.Header.Get("Authorization")); token != "" && a.tokenEqual(token) {
		return true
	}
	if _, password, ok := r.BasicAuth(); ok && a.tokenEqual(password) {
		return true
	}
	if token := r.Header.Get("X-LLM-Gateway-Token"); token != "" && a.tokenEqual(token) {
		return true
	}
	if cookie, err := r.Cookie(accessCookieName); err == nil && a.tokenEqual(cookie.Value) {
		return true
	}
	return false
}

func (a *App) tokenEqual(token string) bool {
	return subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.AccessToken)) == 1
}

func (a *App) setAccessCookie(w http.ResponseWriter, r *http.Request, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     accessCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
		SameSite: http.SameSiteLaxMode,
	})
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
