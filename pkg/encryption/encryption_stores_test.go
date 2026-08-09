package encryption

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoresOmitsUnsetOptionalPasswordReferences(t *testing.T) {
	encoded, err := json.Marshal(Stores{})

	require.NoError(t, err)
	require.NotContains(t, string(encoded), "keystorePasswordSecretRef")
	require.NotContains(t, string(encoded), "truststorePasswordSecretRef")
}
