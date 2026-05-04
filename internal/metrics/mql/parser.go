// Package mql implements a parser and executor for a subset of GCP's
// Monitoring Query Language (MQL).
//
// The parser is driven by grammar.ebnf via gluon's lexkit engine:
//
//  1. grammar.ebnf is parsed once at startup into a GrammarDescriptor.
//  2. Each call to Parse runs lexkit.ParseASTWithOptions, which uses
//     the grammar + MQL-specific TokenMatchers to produce a CST.
//  3. buildQueryPlan walks the CST and emits a typed *mqlpb.QueryPlan.
//
// # Gluon v2 migration path
//
// All CST-walking code (buildQueryPlan and every helper it calls) works
// against the cstNode interface, not against gluon's concrete proto types.
// The only v1-specific code lives in Parse() itself (the lexkit call and
// the unconsumed/lastLeafEnd helpers).
//
// When gluon v2 exposes custom TokenMatchers in ParseCST, migration is:
//  1. Add a v2Node adapter that wraps *v2pb.ASTNode and implements cstNode.
//  2. In Parse(), replace the lexkit call with metaparser.ParseCST.
//  3. Wrap the v2 root with v2Node{} instead of v1Node{}.
//  4. Delete v1Node, wrapV1, unconsumed, lastLeafEnd, and the lexkit import.
//
// Usage:
//
//	plan, err := mql.Parse(`
//	    fetch gce_instance::compute.googleapis.com/instance/cpu/utilization
//	    | filter metric.labels.instance_name =~ "prod-.*"
//	    | align mean(1m)
//	    | group_by [metric.labels.zone], mean()
//	    | within 1h
//	`)
package mql

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/accretional/gluon/lexkit"
	v1pb "github.com/accretional/gluon/pb"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
	mqlpb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

//go:embed grammar.ebnf
var grammarEBNF string

// mqlGrammar is parsed once from grammar.ebnf at startup.
var mqlGrammar *v1pb.GrammarDescriptor

func init() {
	var err error
	mqlGrammar, err = lexkit.Parse(grammarEBNF, lexkit.EBNFLex())
	if err != nil {
		panic(fmt.Sprintf("mql: invalid grammar.ebnf: %v", err))
	}
}

// mqlParseOptions configures the lexkit parser for MQL source text.
// All rules are syntactic (whitespace skipped everywhere). Atomic tokens
// (identifiers, numbers, strings) are handled by TokenMatcher functions
// rather than character-by-character grammar rules.
func mqlParseOptions() *lexkit.ASTParseOptions {
	return &lexkit.ASTParseOptions{
		TokenMatchers: map[string]lexkit.TokenMatchFunc{
			"ident":          matchIdent,
			"ident_segment":  matchIdentSegment,
			"digits":         matchDigits,
			"string_literal": matchStringLiteral,
			"number_literal": matchNumberLiteral,
		},
		IsLexical: func(string) bool { return false },
	}
}

// ---------------------------------------------------------------------------
// cstNode — parser-tree abstraction
//
// All CST-walking code uses this interface instead of a concrete proto type.
// Swapping the underlying parser (v1 → v2) requires only a new adapter and
// a one-line change in Parse(); the build* functions below are untouched.
// ---------------------------------------------------------------------------

type cstNode interface {
	getKind() string
	getValue() string
	getChildren() []cstNode
}

// v1Node adapts *v1pb.ASTNodeDescriptor to cstNode.
// This is the only type that references gluon v1 proto types outside of
// Parse(), unconsumed(), and lastLeafEnd().
type v1Node struct{ n *v1pb.ASTNodeDescriptor }

func (v v1Node) getKind() string { return v.n.GetKind() }
func (v v1Node) getValue() string { return v.n.GetValue() }
func (v v1Node) getChildren() []cstNode {
	raw := v.n.GetChildren()
	if len(raw) == 0 {
		return nil
	}
	out := make([]cstNode, len(raw))
	for i, c := range raw {
		out[i] = v1Node{c}
	}
	return out
}

func wrapV1(n *v1pb.ASTNodeDescriptor) cstNode { return v1Node{n} }

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Parse parses an MQL query string into a *mqlpb.QueryPlan.
func Parse(s string) (*mqlpb.QueryPlan, error) {
	src := strings.TrimSpace(s)
	ast, err := lexkit.ParseASTWithOptions(src, "mql", "Query", mqlGrammar, mqlParseOptions())
	if err != nil {
		return nil, fmt.Errorf("mql parse: %w", err)
	}
	// gluon does not enforce input exhaustion — check that all input was consumed.
	if tail := unconsumed(ast.GetRoot(), src); tail != "" {
		return nil, fmt.Errorf("mql parse: unexpected input: %q", tail)
	}
	return buildQueryPlan(wrapV1(ast.GetRoot()))
}

// unconsumed returns the non-whitespace suffix of src that the CST did not cover.
// It works by finding the end offset of the last leaf node and comparing with len(src).
// This operates on the raw v1 AST before wrapping; it is v1-specific and will be
// deleted once Parse() is migrated to gluon v2.
func unconsumed(root *v1pb.ASTNodeDescriptor, src string) string {
	end := lastLeafEnd(root)
	// Skip trailing whitespace from end.
	for end < len(src) && isWS(src[end]) {
		end++
	}
	if end >= len(src) {
		return ""
	}
	tail := src[end:]
	if len(tail) > 40 {
		tail = tail[:40] + "…"
	}
	return tail
}

// lastLeafEnd returns the source offset just past the last leaf value in the tree.
// string_literal tokens include their quotes, so offset+len(value) is always correct.
func lastLeafEnd(node *v1pb.ASTNodeDescriptor) int {
	if v := node.GetValue(); v != "" {
		return int(node.GetLocation().GetOffset()) + len(v)
	}
	end := 0
	for _, child := range node.GetChildren() {
		if ce := lastLeafEnd(child); ce > end {
			end = ce
		}
	}
	return end
}

func isWS(ch byte) bool { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' }

// ---------------------------------------------------------------------------
// TokenMatcher functions
// ---------------------------------------------------------------------------

// matchIdent matches [a-zA-Z_][a-zA-Z0-9_]*
func matchIdent(src string, pos int) (string, int) {
	if pos >= len(src) {
		return "", -1
	}
	if !isIdentStart(src[pos]) {
		return "", -1
	}
	end := pos + 1
	for end < len(src) && isIdentContinue(src[end]) {
		end++
	}
	return src[pos:end], end
}

// matchIdentSegment matches [a-zA-Z_][a-zA-Z0-9_-]* (allows hyphens for metric segments)
func matchIdentSegment(src string, pos int) (string, int) {
	if pos >= len(src) {
		return "", -1
	}
	if !isIdentStart(src[pos]) {
		return "", -1
	}
	end := pos + 1
	for end < len(src) && (isIdentContinue(src[end]) || src[end] == '-') {
		end++
	}
	return src[pos:end], end
}

// matchDigits matches one or more decimal digits.
func matchDigits(src string, pos int) (string, int) {
	if pos >= len(src) || !isDigit(src[pos]) {
		return "", -1
	}
	end := pos
	for end < len(src) && isDigit(src[end]) {
		end++
	}
	return src[pos:end], end
}

// matchStringLiteral matches "..." or '...' and returns the full literal including quotes.
// The quotes are included so that offset+len(value) gives the correct end position.
func matchStringLiteral(src string, pos int) (string, int) {
	if pos >= len(src) {
		return "", -1
	}
	quote := src[pos]
	if quote != '"' && quote != '\'' {
		return "", -1
	}
	end := pos + 1
	for end < len(src) && src[end] != quote && src[end] != '\n' {
		if src[end] == '\\' && end+1 < len(src) {
			end += 2
		} else {
			end++
		}
	}
	if end >= len(src) || src[end] != quote {
		return "", -1
	}
	return src[pos : end+1], end + 1
}

// matchNumberLiteral matches an optional '-' followed by digits and an optional decimal part.
func matchNumberLiteral(src string, pos int) (string, int) {
	if pos >= len(src) {
		return "", -1
	}
	end := pos
	if src[end] == '-' {
		end++
	}
	if end >= len(src) || !isDigit(src[end]) {
		return "", -1
	}
	for end < len(src) && isDigit(src[end]) {
		end++
	}
	if end < len(src) && src[end] == '.' && end+1 < len(src) && isDigit(src[end+1]) {
		end++
		for end < len(src) && isDigit(src[end]) {
			end++
		}
	}
	return src[pos:end], end
}

func isIdentStart(ch byte) bool    { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' }
func isIdentContinue(ch byte) bool { return isIdentStart(ch) || isDigit(ch) }
func isDigit(ch byte) bool         { return ch >= '0' && ch <= '9' }

// ---------------------------------------------------------------------------
// CST → QueryPlan walker
// ---------------------------------------------------------------------------

func buildQueryPlan(root cstNode) (*mqlpb.QueryPlan, error) {
	// root.Kind == "Query"
	// Children: [ FetchExpr, repeat{ Query{"|", PipeOp}... } ]
	plan := &mqlpb.QueryPlan{}

	for _, child := range root.getChildren() {
		switch child.getKind() {
		case "FetchExpr":
			f, err := buildFetchOp(child)
			if err != nil {
				return nil, err
			}
			plan.Fetch = f

		case "repeat":
			for _, iter := range child.getChildren() {
				// Each iter has kind="Query", children=[terminal("|"), PipeOp]
				pipeNode := findKind(iter, "PipeOp")
				if pipeNode == nil {
					continue
				}
				op, dur, isWithin, err := buildPipeOp(pipeNode)
				if err != nil {
					return nil, err
				}
				if isWithin {
					plan.TimeRange = &mqlpb.TimeRange{
						Range: &mqlpb.TimeRange_Relative{Relative: durationpb.New(dur)},
					}
				} else if op != nil {
					plan.Pipeline = append(plan.Pipeline, op)
				}
			}
		}
	}

	if plan.Fetch == nil {
		return nil, fmt.Errorf("mql: query has no fetch expression")
	}
	return plan, nil
}

// buildPipeOp returns (op, duration, isWithin, err).
// isWithin==true means op is nil and duration holds the within value.
func buildPipeOp(pipeNode cstNode) (*mqlpb.PipeOp, time.Duration, bool, error) {
	if len(pipeNode.getChildren()) == 0 {
		return nil, 0, false, fmt.Errorf("mql: empty PipeOp node")
	}
	inner := pipeNode.getChildren()[0]
	switch inner.getKind() {
	case "FilterOp":
		op, err := buildFilterOp(inner)
		return &mqlpb.PipeOp{Op: &mqlpb.PipeOp_Filter{Filter: op}}, 0, false, err
	case "AlignOp":
		op, err := buildAlignOp(inner)
		return &mqlpb.PipeOp{Op: &mqlpb.PipeOp_Align{Align: op}}, 0, false, err
	case "EveryOp":
		op, err := buildEveryOp(inner)
		return &mqlpb.PipeOp{Op: &mqlpb.PipeOp_Every{Every: op}}, 0, false, err
	case "WithinOp":
		dur, err := buildWithinDuration(inner)
		return nil, dur, true, err
	case "GroupByOp":
		op, err := buildGroupByOp(inner)
		return &mqlpb.PipeOp{Op: &mqlpb.PipeOp_GroupBy{GroupBy: op}}, 0, false, err
	}
	return nil, 0, false, fmt.Errorf("mql: unknown pipe op kind %q", inner.getKind())
}

// ---------------------------------------------------------------------------
// Individual op builders
// ---------------------------------------------------------------------------

func buildFetchOp(node cstNode) (*mqlpb.FetchOp, error) {
	// FetchExpr = "fetch", ResourceType, "::", MetricType
	// node.Children = [t("fetch"), ResourceType, t("::"), MetricType]
	resourceNode := findKind(node, "ResourceType")
	metricNode := findKind(node, "MetricType")
	if resourceNode == nil {
		return nil, fmt.Errorf("mql: FetchExpr missing ResourceType")
	}
	if metricNode == nil {
		return nil, fmt.Errorf("mql: FetchExpr missing MetricType")
	}
	return &mqlpb.FetchOp{
		ResourceType: leaves(resourceNode),
		MetricType:   leaves(metricNode),
	}, nil
}

func buildFilterOp(node cstNode) (*mqlpb.FilterOp, error) {
	// FilterOp = "filter", Predicate
	predNode := findKind(node, "Predicate")
	if predNode == nil {
		return nil, fmt.Errorf("mql: FilterOp missing Predicate")
	}
	pred, err := buildPredicate(predNode)
	if err != nil {
		return nil, err
	}
	return &mqlpb.FilterOp{Predicate: pred}, nil
}

func buildAlignOp(node cstNode) (*mqlpb.AlignOp, error) {
	// AlignOp = "align", AlignerName, "(", Duration, ")"
	alignerNode := findKind(node, "AlignerName")
	durNode := findKind(node, "Duration")
	if alignerNode == nil {
		return nil, fmt.Errorf("mql: AlignOp missing AlignerName")
	}
	if durNode == nil {
		return nil, fmt.Errorf("mql: AlignOp missing Duration")
	}
	alignerStr := leaves(alignerNode)
	aligner, ok := alignerByName[alignerStr]
	if !ok {
		return nil, fmt.Errorf("mql: unknown aligner %q", alignerStr)
	}
	dur, err := buildDuration(durNode)
	if err != nil {
		return nil, err
	}
	return &mqlpb.AlignOp{Aligner: aligner, Period: durationpb.New(dur)}, nil
}

func buildEveryOp(node cstNode) (*mqlpb.EveryOp, error) {
	// EveryOp = "every", Duration
	durNode := findKind(node, "Duration")
	if durNode == nil {
		return nil, fmt.Errorf("mql: EveryOp missing Duration")
	}
	dur, err := buildDuration(durNode)
	if err != nil {
		return nil, err
	}
	return &mqlpb.EveryOp{Period: durationpb.New(dur)}, nil
}

func buildWithinDuration(node cstNode) (time.Duration, error) {
	// WithinOp = "within", Duration
	durNode := findKind(node, "Duration")
	if durNode == nil {
		return 0, fmt.Errorf("mql: WithinOp missing Duration")
	}
	return buildDuration(durNode)
}

func buildGroupByOp(node cstNode) (*mqlpb.GroupByOp, error) {
	// GroupByOp = "group_by", "[", LabelList, "]", ",", ReducerName, "(", ")"
	labelListNode := findKind(node, "LabelList")
	reducerNode := findKind(node, "ReducerName")
	if labelListNode == nil {
		return nil, fmt.Errorf("mql: GroupByOp missing LabelList")
	}
	if reducerNode == nil {
		return nil, fmt.Errorf("mql: GroupByOp missing ReducerName")
	}
	labels := buildLabelList(labelListNode)
	reducerStr := leaves(reducerNode)
	reducer, ok := reducerByName[reducerStr]
	if !ok {
		return nil, fmt.Errorf("mql: unknown reducer %q", reducerStr)
	}
	return &mqlpb.GroupByOp{LabelKeys: labels, Reducer: reducer}, nil
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

func buildDuration(node cstNode) (time.Duration, error) {
	// Duration = digits, DurationUnit
	digitsNode := findKind(node, "digits")
	unitNode := findKind(node, "DurationUnit")
	if digitsNode == nil {
		return 0, fmt.Errorf("mql: Duration missing digits")
	}
	if unitNode == nil {
		return 0, fmt.Errorf("mql: Duration missing DurationUnit")
	}
	n, err := strconv.Atoi(digitsNode.getValue())
	if err != nil {
		return 0, fmt.Errorf("mql: invalid duration number %q", digitsNode.getValue())
	}
	unit := leaves(unitNode)
	switch unit {
	case "s":
		return time.Duration(n) * time.Second, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("mql: unknown duration unit %q", unit)
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

func buildLabelList(node cstNode) []string {
	// LabelList = LabelRef, { ",", LabelRef }
	// node.Children = [LabelRef, repeat{ LabelList{",", LabelRef}... }]
	var labels []string
	for _, child := range node.getChildren() {
		switch child.getKind() {
		case "LabelRef":
			labels = append(labels, leaves(child))
		case "repeat":
			for _, iter := range child.getChildren() {
				// iter.Kind == "LabelList", children = [t(","), LabelRef]
				if lref := findKind(iter, "LabelRef"); lref != nil {
					labels = append(labels, leaves(lref))
				}
			}
		}
	}
	return labels
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

func buildPredicate(node cstNode) (*mqlpb.Predicate, error) {
	// Predicate = AndExpr, { "||", AndExpr }
	andExprs := collectKindFromSeqRepeat(node, "AndExpr")
	if len(andExprs) == 0 {
		return nil, fmt.Errorf("mql: empty Predicate")
	}
	if len(andExprs) == 1 {
		return buildAndExpr(andExprs[0])
	}
	operands := make([]*mqlpb.Predicate, 0, len(andExprs))
	for _, ae := range andExprs {
		p, err := buildAndExpr(ae)
		if err != nil {
			return nil, err
		}
		operands = append(operands, p)
	}
	return &mqlpb.Predicate{
		Expr: &mqlpb.Predicate_Logical{
			Logical: &mqlpb.LogicalExpr{Op: mqlpb.LogicalOp_OR, Operands: operands},
		},
	}, nil
}

func buildAndExpr(node cstNode) (*mqlpb.Predicate, error) {
	// AndExpr = UnaryPred, { "&&", UnaryPred }
	unaryPreds := collectKindFromSeqRepeat(node, "UnaryPred")
	if len(unaryPreds) == 0 {
		return nil, fmt.Errorf("mql: empty AndExpr")
	}
	if len(unaryPreds) == 1 {
		return buildUnaryPred(unaryPreds[0])
	}
	operands := make([]*mqlpb.Predicate, 0, len(unaryPreds))
	for _, up := range unaryPreds {
		p, err := buildUnaryPred(up)
		if err != nil {
			return nil, err
		}
		operands = append(operands, p)
	}
	return &mqlpb.Predicate{
		Expr: &mqlpb.Predicate_Logical{
			Logical: &mqlpb.LogicalExpr{Op: mqlpb.LogicalOp_AND, Operands: operands},
		},
	}, nil
}

func buildUnaryPred(node cstNode) (*mqlpb.Predicate, error) {
	// UnaryPred = NotPred | PrimaryPred
	// node.Children[0] is either NotPred or PrimaryPred
	if len(node.getChildren()) == 0 {
		return nil, fmt.Errorf("mql: empty UnaryPred")
	}
	inner := node.getChildren()[0]
	switch inner.getKind() {
	case "NotPred":
		// NotPred = "!", PrimaryPred
		primaryNode := findKind(inner, "PrimaryPred")
		if primaryNode == nil {
			return nil, fmt.Errorf("mql: NotPred missing PrimaryPred")
		}
		p, err := buildPrimaryPred(primaryNode)
		if err != nil {
			return nil, err
		}
		return &mqlpb.Predicate{
			Expr: &mqlpb.Predicate_Not{Not: &mqlpb.NotExpr{Operand: p}},
		}, nil
	case "PrimaryPred":
		return buildPrimaryPred(inner)
	}
	return nil, fmt.Errorf("mql: unexpected UnaryPred inner kind %q", inner.getKind())
}

func buildPrimaryPred(node cstNode) (*mqlpb.Predicate, error) {
	// PrimaryPred = GroupedPred | ComparisonExpr
	// node.Children[0] is either GroupedPred or ComparisonExpr
	if len(node.getChildren()) == 0 {
		return nil, fmt.Errorf("mql: empty PrimaryPred")
	}
	inner := node.getChildren()[0]
	switch inner.getKind() {
	case "GroupedPred":
		// GroupedPred = "(", Predicate, ")"
		predNode := findKind(inner, "Predicate")
		if predNode == nil {
			return nil, fmt.Errorf("mql: GroupedPred missing Predicate")
		}
		return buildPredicate(predNode)
	case "ComparisonExpr":
		return buildComparisonExpr(inner)
	}
	return nil, fmt.Errorf("mql: unexpected PrimaryPred inner kind %q", inner.getKind())
}

func buildComparisonExpr(node cstNode) (*mqlpb.Predicate, error) {
	// ComparisonExpr = LabelRef, CompOp, Value
	// node.Children = [LabelRef, CompOp, Value]
	labelRefNode := findKind(node, "LabelRef")
	compOpNode := findKind(node, "CompOp")
	valueNode := findKind(node, "Value")
	if labelRefNode == nil {
		return nil, fmt.Errorf("mql: ComparisonExpr missing LabelRef")
	}
	if compOpNode == nil {
		return nil, fmt.Errorf("mql: ComparisonExpr missing CompOp")
	}
	if valueNode == nil {
		return nil, fmt.Errorf("mql: ComparisonExpr missing Value")
	}

	labelPath := leaves(labelRefNode)
	op, err := parseCompOp(leaves(compOpNode))
	if err != nil {
		return nil, err
	}
	val, err := buildValue(valueNode)
	if err != nil {
		return nil, err
	}
	return &mqlpb.Predicate{
		Expr: &mqlpb.Predicate_Comparison{
			Comparison: &mqlpb.ComparisonExpr{
				LabelPath: labelPath,
				Op:        op,
				Rhs:       val,
			},
		},
	}, nil
}

func parseCompOp(s string) (mqlpb.ComparisonOp, error) {
	switch s {
	case "=~":
		return mqlpb.ComparisonOp_RE, nil
	case "!~":
		return mqlpb.ComparisonOp_NRE, nil
	case "!=":
		return mqlpb.ComparisonOp_NEQ, nil
	case "<=":
		return mqlpb.ComparisonOp_LTE, nil
	case ">=":
		return mqlpb.ComparisonOp_GTE, nil
	case "=":
		return mqlpb.ComparisonOp_EQ, nil
	case "<":
		return mqlpb.ComparisonOp_LT, nil
	case ">":
		return mqlpb.ComparisonOp_GT, nil
	}
	return 0, fmt.Errorf("mql: unknown comparison operator %q", s)
}

func buildValue(node cstNode) (*mqlpb.Value, error) {
	// Value = string_literal | number_literal | "true" | "false"
	// node.Children[0] is the matched alternative
	if len(node.getChildren()) == 0 {
		return nil, fmt.Errorf("mql: empty Value node")
	}
	inner := node.getChildren()[0]
	switch inner.getKind() {
	case "string_literal":
		v := inner.getValue()
		if len(v) >= 2 {
			v = v[1 : len(v)-1] // strip surrounding quotes
		}
		return &mqlpb.Value{V: &mqlpb.Value_StringValue{StringValue: v}}, nil
	case "number_literal":
		f, err := strconv.ParseFloat(inner.getValue(), 64)
		if err != nil {
			return nil, fmt.Errorf("mql: invalid number %q", inner.getValue())
		}
		return &mqlpb.Value{V: &mqlpb.Value_NumberValue{NumberValue: f}}, nil
	case "terminal":
		switch inner.getValue() {
		case "true":
			return &mqlpb.Value{V: &mqlpb.Value_BoolValue{BoolValue: true}}, nil
		case "false":
			return &mqlpb.Value{V: &mqlpb.Value_BoolValue{BoolValue: false}}, nil
		}
	}
	return nil, fmt.Errorf("mql: unexpected Value kind %q (value=%q)", inner.getKind(), inner.getValue())
}

// ---------------------------------------------------------------------------
// CST helper functions
// ---------------------------------------------------------------------------

// findKind returns the first direct child of node with the given kind,
// or nil if no such child exists.
func findKind(node cstNode, kind string) cstNode {
	for _, child := range node.getChildren() {
		if child.getKind() == kind {
			return child
		}
	}
	return nil
}

// leaves concatenates all leaf values (nodes with non-empty Value) in the
// subtree in document order. Used to reconstruct paths and tokens.
func leaves(node cstNode) string {
	if node == nil {
		return ""
	}
	if v := node.getValue(); v != "" {
		return v
	}
	var sb strings.Builder
	for _, child := range node.getChildren() {
		sb.WriteString(leaves(child))
	}
	return sb.String()
}

// collectKindFromSeqRepeat collects nodes of `kind` from the pattern:
//
//	Rule = <Kind>, { <sep>, <Kind> }
//
// which produces a node with direct children [<Kind>, repeat{ Rule{sep, <Kind>}... }].
func collectKindFromSeqRepeat(node cstNode, kind string) []cstNode {
	var result []cstNode
	for _, child := range node.getChildren() {
		if child.getKind() == kind {
			result = append(result, child)
		} else if child.getKind() == "repeat" {
			for _, iter := range child.getChildren() {
				if found := findKind(iter, kind); found != nil {
					result = append(result, found)
				}
			}
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Aligner / Reducer name tables
// ---------------------------------------------------------------------------

var alignerByName = map[string]pb.Aligner{
	"mean":           pb.Aligner_ALIGN_MEAN,
	"min":            pb.Aligner_ALIGN_MIN,
	"max":            pb.Aligner_ALIGN_MAX,
	"sum":            pb.Aligner_ALIGN_SUM,
	"count":          pb.Aligner_ALIGN_COUNT,
	"stddev":         pb.Aligner_ALIGN_STDDEV,
	"rate":           pb.Aligner_ALIGN_RATE,
	"delta":          pb.Aligner_ALIGN_DELTA,
	"interpolate":    pb.Aligner_ALIGN_INTERPOLATE,
	"next_older":     pb.Aligner_ALIGN_NEXT_OLDER,
	"percent_change": pb.Aligner_ALIGN_PERCENT_CHANGE,
}

var reducerByName = map[string]pb.Reducer{
	"mean":   pb.Reducer_REDUCE_MEAN,
	"min":    pb.Reducer_REDUCE_MIN,
	"max":    pb.Reducer_REDUCE_MAX,
	"sum":    pb.Reducer_REDUCE_SUM,
	"count":  pb.Reducer_REDUCE_COUNT,
	"stddev": pb.Reducer_REDUCE_STDDEV,
}
