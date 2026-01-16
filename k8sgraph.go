package k8sgraph

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Config holds configuration for K8s graph discovery and storage.
type Config struct {
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	Neo4jToken    string
	Neo4jDatabase string // optional

	ZepAPIKey    string
	ZepUserID    string
	ZepGraphID   string
	ZepGraphName string

	Kubeconfig    string // empty: in-cluster then default
	Namespaces    []string
	AllNamespaces bool

	DryRun                   bool
	MaxPerResource           int      // 0 = no limit
	EventTypes               []string // empty = allow all
	MaxEventFacts            int      // 0 = no limit
	ProprietaryImagePrefixes []string // comma-separated env; empty = classify all as third_party
	IncludeStatus            bool
	MaxStatusConditions      int // 0 = default
	Debug                    bool
	VerifyOnly               bool
	TokenOnly                bool
}

// NodeRow represents a graph node.
type NodeRow struct {
	UID    string         `json:"uid"`
	Labels []string       `json:"labels"`
	Props  map[string]any `json:"props"`
}

// RelRow represents a graph relationship.
type RelRow struct {
	SrcUID string `json:"src_uid"`
	DstUID string `json:"dst_uid"`
	Type   string `json:"type"`
}

// DiscoverGraph discovers K8s resources (incl. CRDs), fetches objects, and returns nodes/relationships.
func DiscoverGraph(ctx context.Context, cfg Config) ([]NodeRow, []RelRow, error) {
	restCfg, err := kubeREST(cfg.Kubeconfig)
	if err != nil {
		return nil, nil, fmt.Errorf("kube config: %w", err)
	}
	disco, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("discovery: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("dynamic: %w", err)
	}

	resLists, err := disco.ServerPreferredResources()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		return nil, nil, fmt.Errorf("discover resources: %w", err)
	}

	nsAllow := sets.New[string]()
	if !cfg.AllNamespaces && len(cfg.Namespaces) > 0 {
		for _, n := range cfg.Namespaces {
			nsAllow.Insert(n)
		}
	}

	if cfg.Debug {
		log.Printf("[k8sgraph] Discovering server preferred resources...")
	}
	discStart := time.Now()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		return nil, nil, fmt.Errorf("discover resources: %w", err)
	}
	if cfg.Debug {
		log.Printf("[k8sgraph] Discovery done in %s, groups: %d", time.Since(discStart), len(resLists))
	}

	if cfg.VerifyOnly {
		if cfg.Debug {
			log.Printf("[k8sgraph] Verify-only mode enabled")
		}
		if err := verifyKubernetes(ctx, dyn); err != nil {
			return nil, nil, err
		}
		if cfg.Debug {
			log.Printf("[k8sgraph] Kubernetes API OK")
		}
		return nil, nil, nil
	}

	// First pass: collect nodes and build indices for name->UID
	nodes := make([]NodeRow, 0, 4096)
	resourceLists := make([]struct {
		gv  schema.GroupVersion
		res unstructured.UnstructuredList
		r   metav1.APIResource
	}, 0, 512)

	// Indexes to resolve Pod refs to CM/Secret
	type key struct{ ns, name string }
	cmIndex := map[key]string{}     // (ns,name) -> uid
	secretIndex := map[key]string{} // (ns,name) -> uid

	for _, rl := range resLists {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			continue
		}
		for _, r := range rl.APIResources {
			if strings.Contains(r.Name, "/") || !has(r.Verbs, "list") {
				continue
			}
			// Events are handled separately as textual facts, not as graph nodes/edges.
			if r.Kind == "Event" {
				continue
			}
			gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name}
			if cfg.Debug {
				log.Printf("[k8sgraph] Listing %s/%s %s (namespaced=%v)", gv.Group, gv.Version, r.Name, r.Namespaced)
			}
			lstStart := time.Now()
			list, err := listAll(ctx, dyn, gvr, r.Namespaced, nsAllow, cfg.MaxPerResource)
			if err != nil {
				if cfg.Debug {
					log.Printf("[k8sgraph] List FAILED %s/%s %s: %v", gv.Group, gv.Version, r.Name, err)
				}
				continue
			}
			if cfg.Debug {
				log.Printf("[k8sgraph] Listed %d %s in %s", len(list.Items), r.Kind, time.Since(lstStart))
			}
			// store for second pass
			resourceLists = append(resourceLists, struct {
				gv  schema.GroupVersion
				res unstructured.UnstructuredList
				r   metav1.APIResource
			}{gv: gv, res: *list, r: r})

			kind := r.Kind
			for i := range list.Items {
				u := list.Items[i]
				uid := string(u.GetUID())
				props := map[string]any{
					"uid":           uid,
					"kind":          kind,
					"apiGroup":      gv.Group,
					"apiVersion":    gv.Version,
					"resource_name": u.GetName(),
					"namespace":     u.GetNamespace(),
					"createdAt":     u.GetCreationTimestamp().Time.UTC().Format(time.RFC3339),
				}
				if cfg.IncludeStatus {
					maxConds := cfg.MaxStatusConditions
					if maxConds <= 0 {
						maxConds = 5
					}
					if st := extractStatusSummary(&u, maxConds); len(st) > 0 {
						props["status"] = st
					}
				}
				if imgs := extractContainerImages(&u, kind); len(imgs) > 0 {
					props["container_images"] = imgs
					props["container_image_class"] = classifyImages(imgs, cfg.ProprietaryImagePrefixes)
				}
				if lbs := u.GetLabels(); len(lbs) > 0 {
					labelPairs := make([]string, 0, len(lbs))
					for k, v := range lbs {
						labelPairs = append(labelPairs, k+"="+v)
					}
					props["labels"] = labelPairs
				}
				if ann := u.GetAnnotations(); len(ann) > 0 {
					annPairs := make([]string, 0, len(ann))
					for k, v := range ann {
						annPairs = append(annPairs, k+"="+v)
					}
					props["annotations"] = annPairs
				}
				nodes = append(nodes, NodeRow{
					UID:    uid,
					Labels: []string{"K8s", kind},
					Props:  props,
				})

				// Build indices for ConfigMap/Secret
				k := key{ns: u.GetNamespace(), name: u.GetName()}
				switch kind {
				case "ConfigMap":
					cmIndex[k] = uid
				case "Secret":
					secretIndex[k] = uid
				}
			}
		}
	}

	if cfg.Debug {
		log.Printf("[k8sgraph] Collected %d nodes from API, building relationships...", len(nodes))
	}
	// Second pass: build relationships
	rels := make([]RelRow, 0, 8192)

	for _, bundle := range resourceLists {
		// OwnerReferences: parent OWNS child
		for i := range bundle.res.Items {
			u := bundle.res.Items[i]
			childUID := string(u.GetUID())
			for _, or := range u.GetOwnerReferences() {
				if or.UID == "" {
					continue
				}
				rels = append(rels, RelRow{
					SrcUID: string(or.UID),
					DstUID: childUID,
					Type:   "OWNS",
				})
			}
		}

		// Service -> Pod via selector
		if bundle.r.Kind == "Service" {
			podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
			for i := range bundle.res.Items {
				svc := bundle.res.Items[i]
				sel := map[string]string{}
				if spec, ok := svc.Object["spec"].(map[string]any); ok {
					if m, ok := spec["selector"].(map[string]any); ok {
						for k, v := range m {
							if s, ok := v.(string); ok {
								sel[k] = s
							}
						}
					}
				}
				if len(sel) == 0 {
					continue
				}
				ns := svc.GetNamespace()
				pods, err := dyn.Resource(podGVR).Namespace(ns).List(ctx, metav1.ListOptions{
					LabelSelector: labels.SelectorFromSet(sel).String(),
				})
				if err != nil {
					continue
				}
				for _, p := range pods.Items {
					rels = append(rels, RelRow{
						SrcUID: string(svc.GetUID()),
						DstUID: string(p.GetUID()),
						Type:   "SELECTS",
					})
				}
			}
		}

		// Pod -> ConfigMap/Secret dependencies
		if bundle.r.Kind == "Pod" {
			for i := range bundle.res.Items {
				pod := bundle.res.Items[i]
				ns := pod.GetNamespace()
				cmNames, secretNames := extractPodRefs(&pod)

				for _, nm := range cmNames {
					if uid, ok := cmIndex[key{ns: ns, name: nm}]; ok {
						rels = append(rels, RelRow{
							SrcUID: string(pod.GetUID()),
							DstUID: uid,
							Type:   "USES_CONFIGMAP",
						})
					}
				}
				for _, nm := range secretNames {
					if uid, ok := secretIndex[key{ns: ns, name: nm}]; ok {
						rels = append(rels, RelRow{
							SrcUID: string(pod.GetUID()),
							DstUID: uid,
							Type:   "USES_SECRET",
						})
					}
				}
			}
		}
	}

	if cfg.Debug {
		log.Printf("[k8sgraph] Collected %d nodes and %d relationships", len(nodes), len(rels))
	}
	if cfg.DryRun {
		fmt.Printf("DryRun: %d nodes, %d relationships\n", len(nodes), len(rels))
		if cfg.Debug {
			log.Printf("[k8sgraph] DryRun complete")
		}
		return nodes, rels, nil
	}

	return nodes, rels, nil
}

// DiscoverEventFacts fetches Kubernetes Events and formats them as short fact strings.
// These are intended for ingestion as text facts, not as graph nodes/edges.
func DiscoverEventFacts(ctx context.Context, cfg Config) ([]string, error) {
	restCfg, err := kubeREST(cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kube config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic: %w", err)
	}

	nsAllow := sets.New[string]()
	if !cfg.AllNamespaces && len(cfg.Namespaces) > 0 {
		for _, n := range cfg.Namespaces {
			nsAllow.Insert(n)
		}
	}

	// Try both legacy core/v1 Events and the newer events.k8s.io API.
	gvrCandidates := []schema.GroupVersionResource{
		{Group: "", Version: "v1", Resource: "events"},
		{Group: "events.k8s.io", Version: "v1", Resource: "events"},
	}

	facts := make([]string, 0, 1024)
	allowType := map[string]struct{}{}
	if len(cfg.EventTypes) > 0 {
		for _, t := range cfg.EventTypes {
			if tt := strings.TrimSpace(strings.ToLower(t)); tt != "" {
				allowType[tt] = struct{}{}
			}
		}
	}
	for _, gvr := range gvrCandidates {
		lst, err := listAll(ctx, dyn, gvr, true, nsAllow, cfg.MaxPerResource)
		if err != nil {
			continue
		}
		for i := range lst.Items {
			if cfg.MaxEventFacts > 0 && len(facts) >= cfg.MaxEventFacts {
				return facts, nil
			}
			e := lst.Items[i]
			uid := string(e.GetUID())
			ns := e.GetNamespace()
			reason, _, _ := unstructured.NestedString(e.Object, "reason")
			msg, _, _ := unstructured.NestedString(e.Object, "message")
			typ, _, _ := unstructured.NestedString(e.Object, "type")
			if len(allowType) > 0 {
				if _, ok := allowType[strings.ToLower(strings.TrimSpace(typ))]; !ok {
					continue
				}
			}
			count, _, _ := unstructured.NestedInt64(e.Object, "count")

			objKind, _, _ := unstructured.NestedString(e.Object, "involvedObject", "kind")
			objName, _, _ := unstructured.NestedString(e.Object, "involvedObject", "name")
			objNS, _, _ := unstructured.NestedString(e.Object, "involvedObject", "namespace")
			if objNS == "" {
				objNS = ns
			}

			ts := e.GetCreationTimestamp().Time.UTC().Format(time.RFC3339)
			if ts == "0001-01-01T00:00:00Z" {
				if s, ok := firstEventTimestamp(&e); ok {
					ts = s
				}
			}

			// Keep this short and stable to be LLM-friendly.
			fact := fmt.Sprintf("K8sEvent[%s] ns=%s type=%s reason=%s involved=%s/%s msg=%q count=%d uid=%s",
				ts, objNS, typ, reason, objKind, objName, msg, count, uid,
			)
			facts = append(facts, fact)
		}
	}

	return facts, nil
}

func firstEventTimestamp(e *unstructured.Unstructured) (string, bool) {
	// Different API versions place timestamps differently. Best-effort extraction.
	// core/v1: firstTimestamp/lastTimestamp; events.k8s.io: eventTime.
	if s, ok := nestedString(e.Object, "eventTime"); ok && strings.TrimSpace(s) != "" {
		return strings.TrimSpace(s), true
	}
	if s, ok := nestedString(e.Object, "lastTimestamp"); ok && strings.TrimSpace(s) != "" {
		return strings.TrimSpace(s), true
	}
	if s, ok := nestedString(e.Object, "firstTimestamp"); ok && strings.TrimSpace(s) != "" {
		return strings.TrimSpace(s), true
	}
	return "", false
}

// WriteNeo4j writes nodes and relationships to Neo4j.
func WriteNeo4j(ctx context.Context, cfg Config, nodes []NodeRow, rels []RelRow) error {
	if cfg.Debug {
		log.Printf("[k8sgraph] Connecting to Neo4j uri=%s db=%q", cfg.Neo4jURI, cfg.Neo4jDatabase)
	}
	auth := neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, "")
	if cfg.TokenOnly {
		auth = neo4j.BearerAuth(cfg.Neo4jToken)
	}
	driver, err := neo4j.NewDriverWithContext(cfg.Neo4jURI, auth)
	if err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j driver: %w", err)
	}
	defer driver.Close(ctx)
	if cfg.Debug {
		log.Printf("[k8sgraph] Verifying connectivity...")
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j connectivity: %w", err)
	}
	if cfg.Debug {
		log.Printf("[k8sgraph] Connectivity OK")
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: cfg.Neo4jDatabase})
	defer session.Close(ctx)

	const chunk = 1000
	for i := 0; i < len(nodes); i += chunk {
		end := min(i+chunk, len(nodes))
		if cfg.Debug {
			log.Printf("[k8sgraph] Writing nodes %d..%d/%d", i, end, len(nodes))
		}
		if err := writeNodes(ctx, session, nodes[i:end]); err != nil {
			return err
		}
	}
	for i := 0; i < len(rels); i += chunk {
		end := min(i+chunk, len(rels))
		if cfg.Debug {
			log.Printf("[k8sgraph] Writing relationships %d..%d/%d", i, end, len(rels))
		}
		if err := writeRels(ctx, session, rels[i:end]); err != nil {
			return err
		}
	}
	if cfg.Debug {
		log.Printf("[k8sgraph] Finished writing graph to Neo4j uri=%s db=%q", cfg.Neo4jURI, cfg.Neo4jDatabase)
		log.Printf("[k8sgraph] Graph build complete: nodes=%d rels=%d", len(nodes), len(rels))
	}
	return nil
}

// BuildGraph discovers K8s resources and writes them to Neo4j.
func BuildGraph(ctx context.Context, cfg Config) error {
	if cfg.VerifyOnly {
		restCfg, err := kubeREST(cfg.Kubeconfig)
		if err != nil {
			return fmt.Errorf("kube config: %w", err)
		}
		dyn, err := dynamic.NewForConfig(restCfg)
		if err != nil {
			return fmt.Errorf("dynamic: %w", err)
		}
		if err := verifyKubernetes(ctx, dyn); err != nil {
			return err
		}
		if err := verifyNeo4j(ctx, cfg); err != nil {
			return err
		}
		if cfg.Debug {
			log.Printf("[k8sgraph] Kubernetes + Neo4j connectivity verified (verify-only mode)")
		}
		return nil
	}

	nodes, rels, err := DiscoverGraph(ctx, cfg)
	if err != nil {
		return err
	}
	if cfg.DryRun {
		return nil
	}
	return WriteNeo4j(ctx, cfg, nodes, rels)
}

// BuildExampleGraph creates example nodes in Neo4j for testing.
func BuildExampleGraph(ctx context.Context, cfg Config) error {
	if cfg.DryRun {
		fmt.Printf("DryRun example: 2 nodes, 0 relationships\n")
		return nil
	}

	if cfg.Debug {
		log.Printf("[k8sgraph] Connecting to Neo4j for example graph uri=%s db=%q", cfg.Neo4jURI, cfg.Neo4jDatabase)
	}
	auth := neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, "")
	if cfg.TokenOnly {
		auth = neo4j.BearerAuth(cfg.Neo4jToken)
	}
	driver, err := neo4j.NewDriverWithContext(cfg.Neo4jURI, auth)
	if err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j driver: %w", err)
	}
	defer driver.Close(ctx)
	if cfg.Debug {
		log.Printf("[k8sgraph] Verifying connectivity for example graph...")
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j connectivity: %w", err)
	}
	if cfg.Debug {
		log.Printf("[k8sgraph] Connectivity OK for example graph")
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: cfg.Neo4jDatabase})
	defer session.Close(ctx)

	nodes := []NodeRow{
		{
			UID:    "example-pod-1-uid",
			Labels: []string{"K8s", "Pod"},
			Props: map[string]any{
				"uid":           "example-pod-1-uid",
				"kind":          "Pod",
				"apiGroup":      "",
				"apiVersion":    "v1",
				"resource_name": "example-pod-1",
				"namespace":     "default",
				"createdAt":     time.Now().UTC().Format(time.RFC3339),
			},
		},
		{
			UID:    "example-pod-2-uid",
			Labels: []string{"K8s", "Pod"},
			Props: map[string]any{
				"uid":        "example-pod-2-uid",
				"kind":       "Pod",
				"apiGroup":   "",
				"apiVersion": "v1",
				"name":       "example-pod-2",
				"namespace":  "default",
				"createdAt":  time.Now().UTC().Format(time.RFC3339),
			},
		},
	}

	rels := []RelRow{}

	if cfg.Debug {
		log.Printf("[k8sgraph] Writing example nodes (%d) and relationships (%d)", len(nodes), len(rels))
	}
	if err := writeNodes(ctx, session, nodes); err != nil {
		return err
	}
	if len(rels) > 0 {
		if err := writeRels(ctx, session, rels); err != nil {
			return err
		}
	}
	if cfg.Debug {
		log.Printf("[k8sgraph] Example graph build complete: nodes=%d rels=%d", len(nodes), len(rels))
	}
	return nil
}

func extractPodRefs(pod *unstructured.Unstructured) (cmNames []string, secretNames []string) {
	// envFrom, env[].valueFrom
	if containers, ok := nestedSlice(pod.Object, "spec", "containers"); ok {
		for _, c := range containers {
			m, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if envFrom, ok := m["envFrom"].([]any); ok {
				for _, e := range envFrom {
					if em, ok := e.(map[string]any); ok {
						if cmRef, ok := em["configMapRef"].(map[string]any); ok {
							if name, _ := cmRef["name"].(string); name != "" {
								cmNames = append(cmNames, name)
							}
						}
						if secRef, ok := em["secretRef"].(map[string]any); ok {
							if name, _ := secRef["name"].(string); name != "" {
								secretNames = append(secretNames, name)
							}
						}
					}
				}
			}
			if env, ok := m["env"].([]any); ok {
				for _, e := range env {
					if em, ok := e.(map[string]any); ok {
						if vf, ok := em["valueFrom"].(map[string]any); ok {
							if cmkr, ok := vf["configMapKeyRef"].(map[string]any); ok {
								if name, _ := cmkr["name"].(string); name != "" {
									cmNames = append(cmNames, name)
								}
							}
							if skr, ok := vf["secretKeyRef"].(map[string]any); ok {
								if name, _ := skr["name"].(string); name != "" {
									secretNames = append(secretNames, name)
								}
							}
						}
					}
				}
			}
		}
	}
	// volumes and projected
	if vols, ok := nestedSlice(pod.Object, "spec", "volumes"); ok {
		for _, v := range vols {
			vm, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if cm, ok := vm["configMap"].(map[string]any); ok {
				if name, _ := cm["name"].(string); name != "" {
					cmNames = append(cmNames, name)
				}
			}
			if sec, ok := vm["secret"].(map[string]any); ok {
				if name, _ := sec["secretName"].(string); name != "" {
					secretNames = append(secretNames, name)
				}
			}
			if proj, ok := vm["projected"].(map[string]any); ok {
				if sources, ok := proj["sources"].([]any); ok {
					for _, s := range sources {
						sm, ok := s.(map[string]any)
						if !ok {
							continue
						}
						if cm, ok := sm["configMap"].(map[string]any); ok {
							if name, _ := cm["name"].(string); name != "" {
								cmNames = append(cmNames, name)
							}
						}
						if sec, ok := sm["secret"].(map[string]any); ok {
							if name, _ := sec["name"].(string); name != "" {
								secretNames = append(secretNames, name)
							}
						}
					}
				}
			}
		}
	}
	// dedupe
	cmNames = dedupe(cmNames)
	secretNames = dedupe(secretNames)
	return
}

func nestedSlice(m map[string]any, path ...string) ([]any, bool) {
	cur := any(m)
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur = mm[p]
	}
	arr, ok := cur.([]any)
	return arr, ok
}

func nestedString(m map[string]any, path ...string) (string, bool) {
	cur := any(m)
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur = mm[p]
	}
	s, ok := cur.(string)
	return s, ok
}

func extractContainerImages(u *unstructured.Unstructured, kind string) []string {
	// Pod: spec.containers / spec.initContainers
	// Workloads (Deployment/StatefulSet/DaemonSet/Job/ReplicaSet/...): spec.template.spec.*
	// CronJob: spec.jobTemplate.spec.template.spec.*
	var specs []map[string]any
	obj := u.Object

	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return nil
	}

	if kind == "Pod" {
		specs = append(specs, spec)
	} else if kind == "CronJob" {
		if jt, ok := spec["jobTemplate"].(map[string]any); ok {
			if jtSpec, ok := jt["spec"].(map[string]any); ok {
				if tpl, ok := jtSpec["template"].(map[string]any); ok {
					if tplSpec, ok := tpl["spec"].(map[string]any); ok {
						specs = append(specs, tplSpec)
					}
				}
			}
		}
	} else {
		if tpl, ok := spec["template"].(map[string]any); ok {
			if tplSpec, ok := tpl["spec"].(map[string]any); ok {
				specs = append(specs, tplSpec)
			}
		}
	}

	images := make([]string, 0, 4)
	seen := map[string]struct{}{}
	for _, s := range specs {
		images = appendImages(images, seen, s, "initContainers")
		images = appendImages(images, seen, s, "containers")
		images = appendImages(images, seen, s, "ephemeralContainers")
	}
	return images
}

func appendImages(dst []string, seen map[string]struct{}, spec map[string]any, key string) []string {
	arr, ok := spec[key].([]any)
	if !ok {
		return dst
	}
	for _, it := range arr {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		img, _ := m["image"].(string)
		img = strings.TrimSpace(img)
		if img == "" {
			continue
		}
		if _, ok := seen[img]; ok {
			continue
		}
		seen[img] = struct{}{}
		dst = append(dst, img)
	}
	return dst
}

func classifyImages(images []string, proprietaryPrefixes []string) map[string]string {
	out := make(map[string]string, len(images))
	for _, img := range images {
		out[img] = classifyImage(img, proprietaryPrefixes)
	}
	return out
}

func classifyImage(image string, proprietaryPrefixes []string) string {
	img := strings.TrimSpace(image)
	for _, p := range proprietaryPrefixes {
		pp := strings.TrimSpace(p)
		if pp == "" {
			continue
		}
		if strings.HasPrefix(img, pp) {
			return "proprietary"
		}
	}

	// Heuristic: well-known shared registries default to third_party/shared.
	reg := imageRegistry(img)
	switch reg {
	case "docker.io", "index.docker.io", "ghcr.io", "gcr.io", "quay.io", "registry.k8s.io":
		return "third_party"
	default:
		// Unknown private registry could still be proprietary, but without prefixes we assume third_party.
		return "third_party"
	}
}

func imageRegistry(image string) string {
	// Best-effort parse: if first component contains '.' or ':' or is 'localhost', treat it as registry.
	// Otherwise assume docker hub.
	parts := strings.Split(image, "/")
	if len(parts) == 0 {
		return "docker.io"
	}
	first := parts[0]
	if strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost" {
		return first
	}
	return "docker.io"
}

func extractStatusSummary(u *unstructured.Unstructured, maxConditions int) map[string]any {
	status, ok := u.Object["status"].(map[string]any)
	if !ok || len(status) == 0 {
		return nil
	}

	out := map[string]any{}

	// Common fields across many types
	if v, ok := status["phase"].(string); ok && strings.TrimSpace(v) != "" {
		out["phase"] = v
	}
	if v, ok := status["reason"].(string); ok && strings.TrimSpace(v) != "" {
		out["reason"] = v
	}
	if v, ok := status["message"].(string); ok && strings.TrimSpace(v) != "" {
		out["message"] = v
	}
	if v, ok := status["observedGeneration"].(int64); ok {
		out["observed_generation"] = v
	}
	if v, ok := status["nodeName"].(string); ok && strings.TrimSpace(v) != "" {
		out["node_name"] = v
	}
	if v, ok := status["podIP"].(string); ok && strings.TrimSpace(v) != "" {
		out["pod_ip"] = v
	}
	if v, ok := status["hostIP"].(string); ok && strings.TrimSpace(v) != "" {
		out["host_ip"] = v
	}

	// Common replica-ish counts (present depending on kind)
	copyInt := func(k, outKey string) {
		if vv, ok := status[k]; ok {
			switch t := vv.(type) {
			case int64:
				out[outKey] = t
			case int:
				out[outKey] = int64(t)
			case float64:
				out[outKey] = int64(t)
			}
		}
	}
	copyInt("replicas", "replicas")
	copyInt("readyReplicas", "ready_replicas")
	copyInt("availableReplicas", "available_replicas")
	copyInt("updatedReplicas", "updated_replicas")
	copyInt("currentReplicas", "current_replicas")
	copyInt("numberReady", "number_ready")
	copyInt("currentNumberScheduled", "current_number_scheduled")
	copyInt("desiredNumberScheduled", "desired_number_scheduled")
	copyInt("numberAvailable", "number_available")

	// Conditions: keep compact and capped
	if arr, ok := status["conditions"].([]any); ok && len(arr) > 0 {
		conds := make([]map[string]any, 0, min(maxConditions, len(arr)))
		for i := 0; i < len(arr) && len(conds) < maxConditions; i++ {
			m, ok := arr[i].(map[string]any)
			if !ok {
				continue
			}
			c := map[string]any{}
			if v, ok := m["type"].(string); ok && v != "" {
				c["type"] = v
			}
			if v, ok := m["status"].(string); ok && v != "" {
				c["status"] = v
			}
			if v, ok := m["reason"].(string); ok && v != "" {
				c["reason"] = v
			}
			if v, ok := m["message"].(string); ok && v != "" {
				// Keep messages short to avoid bloating payloads.
				if len(v) > 300 {
					v = v[:300]
				}
				c["message"] = v
			}
			if v, ok := m["lastTransitionTime"].(string); ok && v != "" {
				c["last_transition_time"] = v
			}
			if len(c) > 0 {
				conds = append(conds, c)
			}
		}
		if len(conds) > 0 {
			out["conditions"] = conds
			out["conditions_count"] = int64(len(arr))
		}
	}

	return out
}

func kubeREST(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
}

func listAll(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, namespaced bool, nsAllow sets.Set[string], max int) (*unstructured.UnstructuredList, error) {
	opts := metav1.ListOptions{}
	if namespaced && nsAllow.Len() > 0 {
		combined := &unstructured.UnstructuredList{}
		for ns := range nsAllow {
			lst, err := dyn.Resource(gvr).Namespace(ns).List(ctx, opts)
			if err != nil {
				continue
			}
			combined.Items = append(combined.Items, lst.Items...)
			if max > 0 && len(combined.Items) >= max {
				break
			}
		}
		return combined, nil
	}
	ns := metav1.NamespaceAll
	if !namespaced {
		ns = ""
	}
	lst, err := dyn.Resource(gvr).Namespace(ns).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if max > 0 && len(lst.Items) > max {
		lst.Items = lst.Items[:max]
	}
	return lst, nil
}

func writeNodes(ctx context.Context, s neo4j.SessionWithContext, rows []NodeRow) error {
	cypher := `
UNWIND $rows AS row
CALL apoc.merge.node(row.labels, {uid: row.uid}, row.props, row.props) YIELD node
RETURN count(*) AS upserts
`
	paramRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		paramRows[i] = map[string]any{
			"uid":    r.UID,
			"labels": r.Labels,
			"props":  r.Props,
		}
	}
	_, err := s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, cypher, map[string]any{"rows": paramRows})
		return nil, err
	})
	return err
}

func writeRels(ctx context.Context, s neo4j.SessionWithContext, rows []RelRow) error {
	cypher := `
UNWIND $rows AS row
MATCH (src:K8s {uid: row.src_uid})
MATCH (dst:K8s {uid: row.dst_uid})
CALL apoc.merge.relationship(src, row.type, {}, {}, dst) YIELD rel
RETURN count(*) AS upserts
`
	paramRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		paramRows[i] = map[string]any{
			"src_uid": r.SrcUID,
			"dst_uid": r.DstUID,
			"type":    r.Type,
		}
	}
	_, err := s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, cypher, map[string]any{"rows": paramRows})
		return nil, err
	})
	return err
}

func verifyNeo4j(ctx context.Context, cfg Config) error {
	driver, err := neo4j.NewDriverWithContext(cfg.Neo4jURI, neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, ""))
	if err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j driver: %w", err)
	}
	defer driver.Close(ctx)
	if err := driver.VerifyConnectivity(ctx); err != nil {
		logNeo4jAuthIssue(err, cfg)
		return fmt.Errorf("neo4j connectivity: %w", err)
	}
	return nil
}

func verifyKubernetes(ctx context.Context, dyn dynamic.Interface) error {
	podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	_, err := dyn.Resource(podGVR).Namespace(metav1.NamespaceDefault).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("kubernetes API list pods: %w", err)
	}
	return nil
}

func has(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func dedupe(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// ParseBool parses booleans from envs if needed by callers
func ParseBool(s string, def bool) bool {
	if s == "" {
		return def
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return def
	}
	return b
}

func logNeo4jAuthIssue(err error, cfg Config) {
	if cfg.Debug {
		log.Printf("[k8sgraph] Neo4j auth issue uri=%s user=%s: %v", cfg.Neo4jURI, cfg.Neo4jUser, err)
	} else {
		log.Printf("[k8sgraph] Neo4j auth issue: %v", err)
	}
}
