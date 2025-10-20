#include <Rcpp.h>
using namespace Rcpp;


// [[Rcpp::export]]
List daily_stats_cpp(NumericVector x, IntegerVector group) {
  int n = x.size();
  if (n != group.size()) {
    stop("x and group must be the same length.");
  }

  int gmax = -1;
  for (int i = 0; i < n; i++) {
    if (group[i] > gmax) {
      gmax = group[i];
    }
  }

  NumericVector max_vals(gmax + 1, NA_REAL);
  NumericVector min_vals(gmax + 1, NA_REAL);
  NumericVector sum_vals(gmax + 1, 0.0);
  IntegerVector count_vals(gmax + 1, 0);

  for (int i = 0; i < n; i++) {
    int grp = group[i];
    double val = x[i];
    if (!NumericVector::is_na(val)) {
      // Max
      if (NumericVector::is_na(max_vals[grp]) || val > max_vals[grp]) {
        max_vals[grp] = val;
      }
      // Min
      if (NumericVector::is_na(min_vals[grp]) || val < min_vals[grp]) {
        min_vals[grp] = val;
      }
      // Sum and count for mean
      sum_vals[grp] += val;
      count_vals[grp]++;
    }
  }

  NumericVector mean_vals(gmax + 1, NA_REAL);
  for (int g = 0; g <= gmax; g++) {
    if (count_vals[g] > 0) {
      mean_vals[g] = sum_vals[g] / count_vals[g];
    }
  }

  return List::create(
    _["max"] = max_vals,
    _["min"] = min_vals,
    _["mean"] = mean_vals
  );
}