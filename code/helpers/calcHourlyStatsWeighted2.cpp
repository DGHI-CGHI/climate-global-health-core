#include <Rcpp.h>
#include <algorithm>
#include <unordered_map>
using namespace Rcpp;

// Weighted median for a single group/variable
static double weighted_median(std::vector<std::pair<double,double>>& vals) {
  if (vals.empty()) return NA_REAL;
  std::sort(vals.begin(), vals.end(),
            [](const std::pair<double,double>& a,
               const std::pair<double,double>& b){ return a.first < b.first; });
  double tot = 0.0;
  for (auto &p : vals) tot += p.second;
  if (tot <= 0.0) return NA_REAL;
  double cum = 0.0;
  for (auto &p : vals) {
    cum += p.second;
    if (cum >= tot * 0.5) return p.first;
  }
  return vals.back().first; // fallback
}

// [[Rcpp::export]]
Rcpp::List hourly_stats_cpp_multi(Rcpp::List vars,
                                  Rcpp::NumericVector weight,
                                  Rcpp::IntegerVector group,
                                  Rcpp::CharacterVector var_names) {

  const int n = group.size();
  const int p = vars.size();

  if (p == 0) stop("vars is empty.");
  if (var_names.size() != p) stop("var_names length must equal length(vars).");
  if (weight.size() != n) stop("weight length must match group length.");

  // Validate all variables have length n and are numeric vectors
  std::vector<Rcpp::NumericVector> X(p);
  for (int v = 0; v < p; ++v) {
    X[v] = as<Rcpp::NumericVector>(vars[v]);
    if (X[v].size() != n) stop("All variables must have the same length as group.");
  }

  // Map arbitrary group ids to [0..G-1], but keep original labels to return
  Rcpp::IntegerVector group_copy = Rcpp::clone(group);
  Rcpp::IntegerVector uniq = Rcpp::sort_unique(group_copy);
  const int G = uniq.size();

  std::unordered_map<int,int> g2i; g2i.reserve(G * 1.3);
  for (int i = 0; i < G; ++i) g2i[uniq[i]] = i;

  // Allocate per-variable results
  std::vector<Rcpp::NumericVector> mean(p,  Rcpp::NumericVector(G, NA_REAL));
  std::vector<Rcpp::NumericVector> vmin(p,  Rcpp::NumericVector(G, NA_REAL));
  std::vector<Rcpp::NumericVector> vmax(p,  Rcpp::NumericVector(G, NA_REAL));
  std::vector<Rcpp::NumericVector> med (p,  Rcpp::NumericVector(G, NA_REAL));

  // Per-variable accumulators
  std::vector<Rcpp::NumericVector> wsum(p,  Rcpp::NumericVector(G, 0.0));
  std::vector<Rcpp::NumericVector> wnum(p,  Rcpp::NumericVector(G, 0.0)); // sum(x*w)

  // For medians: values per group per variable
  std::vector< std::vector< std::vector<std::pair<double,double>> > >
    vals(p, std::vector< std::vector<std::pair<double,double>> >(G));

  // Main pass
  for (int i = 0; i < n; ++i) {
    auto it = g2i.find(group[i]);
    if (it == g2i.end()) continue; // shouldn't happen
    const int gidx = it->second;

    const double w = weight[i];
    if (Rcpp::NumericVector::is_na(w) || w <= 0.0) continue;

    for (int v = 0; v < p; ++v) {
      const double xi = X[v][i];
      if (Rcpp::NumericVector::is_na(xi)) continue;

      wsum[v][gidx] += w;
      wnum[v][gidx] += xi * w;

      // unweighted min/max (common choice; change if you need weighted extrema)
      if (Rcpp::NumericVector::is_na(vmin[v][gidx]) || xi < vmin[v][gidx]) vmin[v][gidx] = xi;
      if (Rcpp::NumericVector::is_na(vmax[v][gidx]) || xi > vmax[v][gidx]) vmax[v][gidx] = xi;

      vals[v][gidx].push_back({xi, w});
    }
  }

  // Finalize means and medians
  for (int v = 0; v < p; ++v) {
    for (int g = 0; g < G; ++g) {
      if (wsum[v][g] > 0.0) {
        mean[v][g] = wnum[v][g] / wsum[v][g];
      }
      if (!vals[v][g].empty()) {
        med[v][g] = weighted_median(vals[v][g]);
      }
    }
  }

  // Build a result shaped like your old API: var_stat names + group_levels for joining
  Rcpp::List out;
  out["group_levels"] = uniq; // original group ids in the order of the result rows

  for (int v = 0; v < p; ++v) {
    std::string base = Rcpp::as<std::string>(var_names[v]);
    out[ base + "_mean"   ] = mean[v];
    out[ base + "_min"    ] = vmin[v];
    out[ base + "_max"    ] = vmax[v];
    out[ base + "_median" ] = med[v];
  }
  return out;
}
