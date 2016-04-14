package admin

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stats Handler", func() {

	Describe("Getting stacks from mock", func() {
		Context("Using mock handler", func() {
			It("should return 200", func() {
				statsHandler := NewStatsHandler(&MockStats{})
				resp := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/", nil)
				Expect(err).NotTo(HaveOccurred())

				statsHandler.ServeHTTP(resp, req)
				Expect(resp.Code).To(Equal(http.StatusOK))
				body, _ := ioutil.ReadAll(resp.Body)
				Expect(string(body)).To(Equal(`{"Foo":"123"}`))
			})

		})

	})
})
