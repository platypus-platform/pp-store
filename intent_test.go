package pp

import (
	"bytes"
	"github.com/platypus-platform/pp-kv-consul"
	"github.com/platypus-platform/pp-logging"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func prepareStore() *ppkv.Client {
	kv, _ := ppkv.NewClient()
	_, err := kv.Get("test")
	if err != nil {
		return nil
	}

	kv.DeleteTree("nodes/testhost")
	kv.DeleteTree("clusters/testapp")

	return kv
}

var _ = Describe("Reading spec from KV store", func() {
	It("Works", func() {
		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/versions", map[string]string{
			"abc123": "prep",
			"def456": "active",
		})
		kv.Put("clusters/testapp/test/deploy_config", map[string]string{
			"basedir": "/sometmp",
		})

		c := make(chan IntentNode)
		s := make([]IntentNode, 0)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range c {
				// This is kind of a roundabout way to get the results into a slice.
				// Surely there is a better way?
				s = append(s, w)
			}
		}()
		err := PollOnce("testhost", c)
		close(c)
		if err != nil {
			Fail(err.Error())
			return
		}
		wg.Wait()

		expected := []IntentNode{
			IntentNode{
				Apps: map[string]IntentApp{
					"testapp": IntentApp{
						Name:    "testapp",
						Basedir: "/sometmp",
						Versions: map[string]string{
							"abc123": "prep",
							"def456": "active",
						},
					},
				},
			},
		}

		Expect(s).To(Equal(expected))
	})

	It("Gracefully handles no data", func() {
		kv := prepareStore()
		if kv == nil {
			return
		}

		PollOnce("testhost", Sink())
	})

	It("Gracefully handles invalid node data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", 34)

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("Invalid node data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing cluster data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"bogus": "test",
		})

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No cluster key"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing or invalid version data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))

		buf.Reset()

		kv.Put("clusters/testapp/test/versions", "bogus")

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles missing or invalid config data", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/versions", map[string]string{
			"abc123": "active",
		})

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))

		buf.Reset()
		kv.Put("clusters/testapp/test/deploy_config", "bogus")
		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("No or invalid data"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})

	It("Gracefully handles non-absolute basedir", func() {
		var buf bytes.Buffer
		logger.SetOut(&buf)
		defer logger.SetOut(logger.DefaultOut())

		kv := prepareStore()
		if kv == nil {
			return
		}

		kv.Put("nodes/testhost/testapp", map[string]string{
			"cluster": "test",
		})
		kv.Put("clusters/testapp/test/versions", map[string]string{
			"abc123": "active",
		})
		kv.Put("clusters/testapp/test/deploy_config", map[string]string{
			"basedir": "relative",
		})

		PollOnce("testhost", Sink())

		Expect(buf.String()).To(ContainSubstring("Not allowing relative basedir"))
		Expect(buf.String()).To(ContainSubstring("testapp"))
	})
})

func Sink() chan IntentNode {
	c := make(chan IntentNode)
	go func() {
		for _ = range c {
		}
	}()
	return c
}
