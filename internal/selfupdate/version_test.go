package selfupdate

import "testing"

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		left, right string
		want        int
	}{
		{"0.2.0-rc40", "0.2.0-rc39", 1},
		{"0.2.0", "0.2.0-rc40", 1},
		{"0.2.1-rc1", "0.2.0", 1},
		{"0.2.0-rc9", "0.2.0-rc10", -1},
		{"v1.0.0", "1.0", 0},
	}
	for _, test := range cases {
		got := compareVersions(test.left, test.right)
		if got != test.want {
			t.Fatalf("compareVersions(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
		}
	}
}
