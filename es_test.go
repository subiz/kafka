package kafka

import (
	"testing"
)

func skipTest(t *testing.T) {
	t.Skip()
}

func TestValidateTopicRule(t *testing.T) {
	skipTest(t)
	var testcase = []struct {
		in string
		out bool
	}{
		{ "ha$ivan", false },
		{ "haivan", true },
		{ "thanh._", true },
		{ "thanh@!#", false },
		{ "", false },
		{ "asdkjfkjasdhfjkhaskjdhjkasdhfkhasjkdhkjhasdkjfhkasdjhfkjashdfkhaskjdfkjashdfkhasdjkfjkasdhfjkhaskdjfhkasjdhfjkhqweriusahdfiuqwhefihasiudhuiqwehifuhsauidfqwjkehfiuashdfjkhqwuiefhuiashdfuhwekjfchkjashedfkadskjfhkjasdhfjkhasdjkfhjkasldfjklashdfjklhasjdkfkjsadhfkjsahdfkjhsadjkfhkjsadhfjkhasdjkfhkasjdhfjkashdfjhasdjkfjsakdh", false  },
	}
	for _, c := range testcase {
		out := validateTopicName(c.in)
		if out != c.out {
			t.Errorf("%s shoule be %v, got %v", c.in, c.out, out)
		}
	}
}
