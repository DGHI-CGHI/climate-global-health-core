#include <Rcpp.h>
using namespace Rcpp;


// [[Rcpp::export]]
NumericVector rollingWeightedSumVec(NumericVector x, NumericVector weights) {
  int n = x.size();
  if (weights.size() != 3) {
    stop("weights must have length 3.");
  }

  NumericVector result(n, NA_REAL);
  for (int i = 1; i < n - 1; i++) {
    double x1 = x[i - 1];
    double x2 = x[i];
    double x3 = x[i + 1];
    if (NumericVector::is_na(x1) || NumericVector::is_na(x2) || NumericVector::is_na(x3)) {
      result[i] = NA_REAL;
    } else {
      double s = (x1 * weights[0]) + (x2 * weights[1]) + (x3 * weights[2]);
      result[i] = s;
    }
  }
  return result;
}