const ColumnPredicateOperation = {
  EXACT_MATCH  : 1,
  PREFIX_MATCH : 2,
  REGEX_MATCH  : 4,
  VALUE_MATCH  : 7,
  QUALIFIER_EXACT_MATCH  : 256,
  QUALIFIER_PREFIX_MATCH : 512,
  QUALIFIER_REGEX_MATCH  : 1024,
  QUALIFIER_MATCH        : 1792
};

module.exports = ColumnPredicateOperation;