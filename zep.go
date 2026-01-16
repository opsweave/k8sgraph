package k8sgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/getzep/zep-go/v3"
	zepclient "github.com/getzep/zep-go/v3/client"
	"github.com/getzep/zep-go/v3/option"
)

type zepEpisode struct {
	Source        string    `json:"source"`
	CollectedAt   time.Time `json:"collected_at"`
	Nodes         []NodeRow `json:"nodes"`
	Relationships []RelRow  `json:"relationships"`
}

// WriteZepSingle writes nodes and relationships to Zep in a single payload.
func WriteZepSingle(ctx context.Context, cfg *Config, nodes []NodeRow, rels []RelRow) error {
	const maxChars = 10000

	if cfg == nil {
		return fmt.Errorf("nil config")
	}
	if cfg.ZepAPIKey == "" {
		return fmt.Errorf("ZEP_API_KEY is required when GRAPH_BACKEND=zep")
	}

	client := zepclient.NewClient(option.WithAPIKey(cfg.ZepAPIKey))

	graphID := cfg.ZepGraphID
	if graphID == "" {
		graphID = "k8s-graph-" + uuid.NewString()
		cfg.ZepGraphID = graphID
	}
	graphName := cfg.ZepGraphName
	if graphName == "" {
		graphName = "k8s-graph"
	}

	_, _ = client.Graph.Create(ctx, &zep.CreateGraphRequest{
		GraphID: graphID,
		Name:    zep.String(graphName),
	})

	payload := zepEpisode{
		Source:        "k8sgraph",
		CollectedAt:   time.Now().UTC(),
		Nodes:         nodes,
		Relationships: rels,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal zep payload: %w", err)
	}
	if len(b) > maxChars {
		return fmt.Errorf("single payload too large (%d chars > %d)", len(b), maxChars)
	}
	_, err = client.Graph.Add(ctx, &zep.AddDataRequest{
		GraphID: zep.String(graphID),
		Type:    zep.GraphDataTypeJSON,
		Data:    string(b),
	})
	if err != nil {
		return fmt.Errorf("zep graph add: %w", err)
	}
	if cfg.Debug {
		fmt.Printf("[k8sgraph] wrote to Zep in single payload (graph_id=%s) nodes=%d relationships=%d\n", graphID, len(nodes), len(rels))
	}
	return nil
}

// WriteZep writes nodes and relationships to Zep in size-limited batches.
func WriteZep(ctx context.Context, cfg *Config, nodes []NodeRow, rels []RelRow) error {
	const maxChars = 10000

	if cfg == nil {
		return fmt.Errorf("nil config")
	}
	if cfg.ZepAPIKey == "" {
		return fmt.Errorf("ZEP_API_KEY is required when GRAPH_BACKEND=zep")
	}

	client := zepclient.NewClient(option.WithAPIKey(cfg.ZepAPIKey))

	graphID := cfg.ZepGraphID
	if graphID == "" {
		graphID = "k8s-graph-" + uuid.NewString()
		cfg.ZepGraphID = graphID
	}
	graphName := cfg.ZepGraphName
	if graphName == "" {
		graphName = "k8s-graph"
	}

	// Best-effort create. If it already exists, the API may return an error; we'll proceed and let
	// subsequent writes be the source of truth.
	_, _ = client.Graph.Create(ctx, &zep.CreateGraphRequest{
		GraphID: graphID,
		Name:    zep.String(graphName),
	})

	send := func(payload zepEpisode) error {
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal zep payload: %w", err)
		}
		if len(b) > maxChars {
			return fmt.Errorf("zep payload too large (%d chars > %d)", len(b), maxChars)
		}
		_, err = client.Graph.Add(ctx, &zep.AddDataRequest{
			GraphID: zep.String(graphID),
			Type:    zep.GraphDataTypeJSON,
			Data:    string(b),
		})
		if err != nil {
			return fmt.Errorf("zep graph add: %w", err)
		}
		return nil
	}

	base := zepEpisode{
		Source:      "k8sgraph",
		CollectedAt: time.Now().UTC(),
	}

	// 1) Send nodes in size-limited batches
	nSent := 0
	for i := 0; i < len(nodes); {
		j := i
		// Ensure we always make progress; if a single node makes the payload exceed the limit,
		// error out with a clear message.
		for j < len(nodes) {
			candidate := base
			candidate.Nodes = nodes[i : j+1]
			candidate.Relationships = nil
			b, err := json.Marshal(candidate)
			if err != nil {
				return fmt.Errorf("marshal zep payload: %w", err)
			}
			if len(b) > maxChars {
				if j == i {
					return fmt.Errorf("single node payload exceeds %d chars; uid=%s", maxChars, nodes[i].UID)
				}
				break
			}
			j++
		}
		batch := base
		batch.Nodes = nodes[i:j]
		batch.Relationships = nil
		if err := send(batch); err != nil {
			return err
		}
		nSent += (j - i)
		i = j
	}

	// 2) Send relationships in size-limited batches
	rSent := 0
	for i := 0; i < len(rels); {
		j := i
		for j < len(rels) {
			candidate := base
			candidate.Nodes = nil
			candidate.Relationships = rels[i : j+1]
			b, err := json.Marshal(candidate)
			if err != nil {
				return fmt.Errorf("marshal zep payload: %w", err)
			}
			if len(b) > maxChars {
				if j == i {
					return fmt.Errorf("single relationship payload exceeds %d chars; src=%s dst=%s type=%s", maxChars, rels[i].SrcUID, rels[i].DstUID, rels[i].Type)
				}
				break
			}
			j++
		}
		batch := base
		batch.Nodes = nil
		batch.Relationships = rels[i:j]
		if err := send(batch); err != nil {
			return err
		}
		rSent += (j - i)
		i = j
	}

	if cfg.Debug {
		fmt.Printf("[k8sgraph] wrote to Zep in chunks (graph_id=%s) nodes=%d relationships=%d\n", graphID, nSent, rSent)
	}
	return nil
}

// WriteZepFacts writes event facts to Zep as text data.
func WriteZepFacts(ctx context.Context, cfg *Config, facts []string) error {
	const maxChars = 10000

	if len(facts) == 0 {
		return nil
	}
	if cfg == nil {
		return fmt.Errorf("nil config")
	}
	if cfg.ZepAPIKey == "" {
		return fmt.Errorf("ZEP_API_KEY is required when GRAPH_BACKEND=zep")
	}

	client := zepclient.NewClient(option.WithAPIKey(cfg.ZepAPIKey))

	graphID := cfg.ZepGraphID
	if graphID == "" {
		return fmt.Errorf("ZEP_GRAPH_ID is required for fact ingestion")
	}

	// Best-effort create (same as WriteZep).
	graphName := cfg.ZepGraphName
	if graphName == "" {
		graphName = "k8s-graph"
	}
	_, _ = client.Graph.Create(ctx, &zep.CreateGraphRequest{
		GraphID: graphID,
		Name:    zep.String(graphName),
	})

	flush := func(buf []byte) error {
		if len(buf) == 0 {
			return nil
		}
		_, err := client.Graph.Add(ctx, &zep.AddDataRequest{
			GraphID: zep.String(graphID),
			Type:    zep.GraphDataTypeText,
			Data:    string(buf),
		})
		if err != nil {
			return fmt.Errorf("zep graph add facts: %w", err)
		}
		return nil
	}

	buf := make([]byte, 0, maxChars)
	for _, f := range facts {
		line := []byte(f + "\n")
		if len(line) > maxChars {
			// If a single fact line is too big, truncate to fit and still send something.
			line = append(line[:maxChars-2], '\n')
		}
		if len(buf)+len(line) > maxChars {
			if err := flush(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		buf = append(buf, line...)
	}
	if err := flush(buf); err != nil {
		return err
	}

	if cfg.Debug {
		fmt.Printf("[k8sgraph] wrote %d event facts to Zep (graph_id=%s)\n", len(facts), graphID)
	}
	return nil
}
