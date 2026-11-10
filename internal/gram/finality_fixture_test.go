package gram

import (
	"encoding/json"
	"net/http"
)

func finalityFixture(w http.ResponseWriter, r *http.Request, rows []rawTransaction) bool {
	if r.URL.Path == "/api/v3/masterchainInfo" {
		json.NewEncoder(w).Encode(map[string]any{"last": map[string]any{"workchain": -1, "seqno": 100}})
		return true
	}
	if r.URL.Path != "/api/v3/transactions" {
		return false
	}
	var out []map[string]any
	for _, raw := range rows {
		out = append(out, map[string]any{"account": DefaultAddress, "hash": raw.ID.Hash, "lt": raw.ID.LT, "now": raw.Utime, "mc_block_seqno": 99, "block_ref": map[string]any{"workchain": 0, "shard": "8000000000000000", "seqno": 100}})
	}
	json.NewEncoder(w).Encode(map[string]any{"transactions": out})
	return true
}
