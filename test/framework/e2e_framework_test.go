package framework

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderOperatorImageTransform(t *testing.T) {
	tests := []struct {
		name   string
		config OperatorDeploymentConfig
		want   string
	}{
		{
			name:   "tag only",
			config: OperatorDeploymentConfig{ImageName: "example/operator", ImageTag: "test"},
			want:   "  - name: k8ssandra/k8ssandra-operator\n    newName: example/operator\n    newTag: test",
		},
		{
			name: "digest overrides tag",
			config: OperatorDeploymentConfig{ImageName: "example/operator", ImageTag: "ignored",
				ImageDigest: "sha256:0123456789abcdef"},
			want: "  - name: k8ssandra/k8ssandra-operator\n    newName: example/operator\n" +
				"    digest: sha256:0123456789abcdef",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, renderOperatorImageTransform(test.config))
		})
	}
}
