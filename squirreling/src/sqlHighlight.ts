import { parseSql, tokenizeSql } from 'squirreling'
import type { ExprNode, SelectStatement, Token } from 'squirreling'

export interface HighlightRange {
  start: number
  end: number
  type: 'keyword' | 'function' | 'string' | 'number' | 'operator' | 'identifier'
}

/**
 * Generate syntax highlighting ranges for a SQL query.
 * Uses tokenizer for basic syntax and AST for semantic info (function names).
 */
export function highlightSql(sql: string): HighlightRange[] {
  let tokens: Token[]
  try {
    tokens = tokenizeSql(sql)
  } catch {
    return [] // If tokenization fails, no highlighting
  }

  // Try to parse for semantic info (function positions)
  const functionPositions = new Set<number>()
  try {
    const ast = parseSql({ query: sql })
    collectFunctionPositions(ast, functionPositions)
  } catch {
    // Parse failed, just use token-based highlighting
  }

  const ranges: HighlightRange[] = []
  for (const token of tokens) {
    if (token.type === 'eof') continue

    let type: HighlightRange['type']
    switch (token.type) {
    case 'keyword':
      type = 'keyword'
      break
    case 'identifier':
      // Check if this identifier is a function name
      type = functionPositions.has(token.positionStart) ? 'function' : 'identifier'
      break
    case 'string':
      type = 'string'
      break
    case 'number':
      type = 'number'
      break
    case 'operator':
      type = 'operator'
      break
    default:
      continue // Skip comma, dot, paren, semicolon
    }

    ranges.push({
      start: token.positionStart,
      end: token.positionEnd,
      type,
    })
  }

  return ranges
}

/**
 * Recursively collect the start positions of all function names in the AST.
 */
function collectFunctionPositions(stmt: SelectStatement, positions: Set<number>): void {
  // Process columns
  for (const col of stmt.columns) {
    if (col.kind === 'derived') {
      collectFromExpr(col.expr, positions)
    }
  }

  // Process FROM subquery
  if (stmt.from.kind === 'subquery') {
    collectFunctionPositions(stmt.from.query, positions)
  }

  // Process JOINs
  for (const join of stmt.joins) {
    if (join.on) {
      collectFromExpr(join.on, positions)
    }
  }

  // Process WHERE
  if (stmt.where) {
    collectFromExpr(stmt.where, positions)
  }

  // Process GROUP BY
  for (const expr of stmt.groupBy) {
    collectFromExpr(expr, positions)
  }

  // Process HAVING
  if (stmt.having) {
    collectFromExpr(stmt.having, positions)
  }

  // Process ORDER BY
  for (const item of stmt.orderBy) {
    collectFromExpr(item.expr, positions)
  }
}

/**
 * Recursively collect function positions from an expression node.
 */
function collectFromExpr(expr: ExprNode, positions: Set<number>): void {
  switch (expr.type) {
  case 'function':
    positions.add(expr.positionStart)
    for (const arg of expr.args) {
      collectFromExpr(arg, positions)
    }
    break
  case 'binary':
    collectFromExpr(expr.left, positions)
    collectFromExpr(expr.right, positions)
    break
  case 'unary':
    collectFromExpr(expr.argument, positions)
    break
  case 'case':
    if (expr.caseExpr) {
      collectFromExpr(expr.caseExpr, positions)
    }
    for (const when of expr.whenClauses) {
      collectFromExpr(when.condition, positions)
      collectFromExpr(when.result, positions)
    }
    if (expr.elseResult) {
      collectFromExpr(expr.elseResult, positions)
    }
    break
  case 'cast':
    collectFromExpr(expr.expr, positions)
    break
  case 'in':
    collectFromExpr(expr.expr, positions)
    collectFunctionPositions(expr.subquery, positions)
    break
  case 'in valuelist':
    collectFromExpr(expr.expr, positions)
    for (const val of expr.values) {
      collectFromExpr(val, positions)
    }
    break
  case 'exists':
  case 'not exists':
    collectFunctionPositions(expr.subquery, positions)
    break
  case 'subquery':
    collectFunctionPositions(expr.subquery, positions)
    break
    // literal, identifier, interval - no recursion needed
  }
}
