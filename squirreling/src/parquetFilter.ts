import type { ParquetQueryFilter } from 'hyparquet'
import type { ParquetQueryOperator } from 'hyparquet/src/types.js'
import type { BinaryNode, BinaryOp, ComparisonOp, ExprNode, InValuesNode, SqlPrimitive } from 'squirreling/src/types.js'

/**
 * Converts a squirreling WHERE clause AST to hyparquet filter format.
 * Returns undefined if the expression cannot be fully converted.
 */
export function whereToParquetFilter(where: ExprNode | undefined): ParquetQueryFilter | undefined {
  if (!where) return undefined
  return convertExpr(where, false)
}

/**
 * Converts an expression node to filter format
 */
function convertExpr(node: ExprNode, negate: boolean): ParquetQueryFilter | undefined {
  if (node.type === 'unary' && node.op === 'NOT') {
    return convertExpr(node.argument, !negate)
  }
  if (node.type === 'binary') {
    return convertBinary(node, negate)
  }
  if (node.type === 'in valuelist') {
    return convertInValues(node, negate)
  }
  if (node.type === 'cast') {
    // TODO: cast
    return convertExpr(node.expr, negate)
  }
  // Non-convertible types - return undefined to skip optimization
  return undefined
}

/**
 * Converts a binary expression to filter format
 */
function convertBinary({ op, left, right }: BinaryNode, negate: boolean): ParquetQueryFilter | undefined {
  if (op === 'AND') {
    const leftFilter = convertExpr(left, negate)
    const rightFilter = convertExpr(right, negate)
    if (!leftFilter || !rightFilter) return
    return negate
      ? { $or: [leftFilter, rightFilter] }
      : { $and: [leftFilter, rightFilter] }
  }
  if (op === 'OR') {
    const leftFilter = convertExpr(left, false)
    const rightFilter = convertExpr(right, false)
    if (!leftFilter || !rightFilter) return
    return negate
      ? { $nor: [leftFilter, rightFilter] }
      : { $or: [leftFilter, rightFilter] }
  }

  // LIKE is not supported by hyparquet filters
  if (op === 'LIKE') return

  // Comparison operators: need identifier on one side and literal on the other
  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return

  // Map SQL operator to MongoDB operator
  const mongoOp = mapOperator(op, flipped, negate)
  if (!mongoOp) return
  return { [column]: { [mongoOp]: value } }
}

/**
 * Extracts column name and literal value from binary operands.
 * Handles both "column op value" and "value op column" patterns.
 */
function extractColumnAndValue(
  left: ExprNode,
  right: ExprNode,
): { column: string | undefined; value: SqlPrimitive | undefined; flipped: boolean } {
  // column op value
  if (left.type === 'identifier' && right.type === 'literal') {
    return { column: left.name, value: right.value, flipped: false }
  }
  // value op column (flipped)
  if (left.type === 'literal' && right.type === 'identifier') {
    return { column: right.name, value: left.value, flipped: true }
  }
  // Neither pattern matches
  return { column: undefined, value: undefined, flipped: false }
}

/**
 * Maps SQL operator to MongoDB operator, accounting for flipped operands
 */
function mapOperator(
  op: BinaryOp,
  flipped: boolean,
  negate: boolean,
): keyof ParquetQueryOperator | undefined {
  if (!isComparisonOp(op)) return

  let mappedOp: ComparisonOp = op
  if (negate) mappedOp = neg(mappedOp)
  if (flipped) mappedOp = flip(mappedOp)
  if (mappedOp === '<') return '$lt'
  if (mappedOp === '<=') return '$lte'
  if (mappedOp === '>') return '$gt'
  if (mappedOp === '>=') return '$gte'
  if (mappedOp === '=') return '$eq'
  return '$ne'
}

function neg(op: ComparisonOp): ComparisonOp {
  if (op === '<') return '>='
  if (op === '<=') return '>'
  if (op === '>') return '<='
  if (op === '>=') return '<'
  if (op === '=') return '!='
  return '='
}

function flip(op: ComparisonOp): ComparisonOp {
  if (op === '<') return '>'
  if (op === '<=') return '>='
  if (op === '>') return '<'
  if (op === '>=') return '<='
  return op
}

export function isComparisonOp(op: string): op is ComparisonOp {
  return ['=', '!=', '<>', '<', '>', '<=', '>='].includes(op)
}

/**
 * Converts IN/NOT IN value list expression to filter format
 */
function convertInValues(node: InValuesNode, negate: boolean): ParquetQueryFilter | undefined {
  if (node.expr.type !== 'identifier') return

  // All values must be literals
  const values: SqlPrimitive[] = []
  for (const val of node.values) {
    if (val.type !== 'literal') return
    values.push(val.value)
  }

  return { [node.expr.name]: { [negate ? '$nin' : '$in']: values } }
}
