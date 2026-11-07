import torch

from .babamul_consumer import BabamulPhotometryConsumer


class TempoPhotometryConsumer(BabamulPhotometryConsumer):
    """A consumer for the Tempo photometry data stream.

    This class is a specialized consumer that inherits from `BabamulPhotometryConsumer`.
    It is designed to handle the specific requirements of the Tempo photometry data stream.
    """

    def __init__(self, config, data_location=None):
        """Initialize the Tempo photometry consumer.

        Parameters
        ----------
        config : dict
            The configuration dictionary for the consumer.
        data_location : str, optional
            The location of the data stream. Defaults to None.
        """
        super().__init__(config=config, data_location=data_location)
        self.feature_set = config["hyrax_alerts"]["consumer"]["TempoPhotometryConsumer"]["feature_set"]

    def get_globals(self, msg):
        """Get the global features from the photometry tensor."""
        photometry_tensor = self.get_photometry(msg)
        global_features = _global_features_from_sequence(
            photometry_tensor,
            band_mode=self.band_mode,
            feature_set=self.feature_set,
        )
        return global_features


def global_feature_dim(feature_set: str) -> int:
    """Return the dimension of the global feature vector based on the feature set."""
    if feature_set == "basic":
        return 8
    if feature_set == "enhanced":
        return 16
    if feature_set == "physics":
        return 24
    raise ValueError(f"Unknown global feature set: {feature_set}")


def _safe_slope(x: torch.Tensor, y: torch.Tensor) -> float:
    """Compute the slope of a linear fit to the data, handling edge cases."""
    n = int(x.numel())
    if n < 2:
        return 0.0
    xm = torch.mean(x)
    ym = torch.mean(y)
    denom = torch.sum((x - xm) ** 2)
    if float(denom.item()) <= 1e-12:
        return 0.0
    slope = torch.sum((x - xm) * (y - ym)) / denom
    return float(slope.item())


def _global_features_from_sequence(
    x: torch.Tensor, *, band_mode: str, feature_set: str = "basic"
) -> torch.Tensor:
    """Compute global features per light curve from unnormalized token sequence."""
    if not isinstance(x, torch.Tensor):
        x = torch.as_tensor(x)
    if x.size(0) == 0:
        # Defensive fallback for any upstream truncation edge case.
        return torch.zeros(global_feature_dim(feature_set), dtype=torch.float32)
    cont = x[:, :4].float()
    dt_first = torch.expm1(cont[:, 0]).clamp_min(0.0)
    dt_prev = torch.expm1(cont[:, 1]).clamp_min(0.0)
    logf = cont[:, 2]

    n_obs = float(x.size(0))
    duration = float(dt_first.max().item() if x.size(0) > 0 else 0.0)
    amp = float((logf.max() - logf.min()).item() if x.size(0) > 0 else 0.0)

    if band_mode == "onehot":
        onehot = x[:, 4:7].float()
        counts = onehot.sum(dim=0)
        band_id = onehot.argmax(dim=1)
    else:
        band_id = x[:, 4].long().clamp(0, 2)
        counts = torch.stack([(band_id == k).sum() for k in range(3)], dim=0).float()

    # Color proxies: average log-flux differences by band.
    means = []
    for k in range(3):
        m = band_id == k
        means.append(logf[m].mean() if m.any() else torch.tensor(0.0, dtype=logf.dtype, device=logf.device))
    color_gr = means[0] - means[1]
    color_ri = means[1] - means[2]

    basic = [
        duration,
        n_obs,
        counts[0].item(),
        counts[1].item(),
        counts[2].item(),
        amp,
        float(color_gr.item()),
        float(color_ri.item()),
    ]
    if feature_set == "basic":
        return torch.tensor(basic, dtype=torch.float32)

    if feature_set not in {"enhanced", "physics"}:
        raise ValueError(f"Unknown global feature set: {feature_set}")

    idx_peak = int(torch.argmax(logf).item())
    peak_t = float(dt_first[idx_peak].item())
    peak_frac_h = peak_t / max(1e-6, duration)
    peak_flux = float(logf[idx_peak].item())
    med_dt_prev = float(torch.median(dt_prev).item())
    std_flux = float(torch.std(logf, unbiased=False).item()) if x.size(0) > 1 else 0.0
    p90 = float(torch.quantile(logf, 0.90).item()) if x.size(0) > 1 else peak_flux
    p10 = float(torch.quantile(logf, 0.10).item()) if x.size(0) > 1 else peak_flux

    t = dt_first
    rise_mask = t <= t[idx_peak]
    fall_mask = t >= t[idx_peak]
    rise_slope = _safe_slope(t[rise_mask], logf[rise_mask])
    fall_slope = _safe_slope(t[fall_mask], logf[fall_mask])
    rise_fall_ratio = rise_slope / max(1e-6, abs(fall_slope))

    enhanced = basic + [
        peak_frac_h,
        peak_flux,
        med_dt_prev,
        std_flux,
        p90 - p10,
        rise_slope,
        fall_slope,
        rise_fall_ratio,
    ]
    if feature_set == "enhanced":
        return torch.tensor(enhanced, dtype=torch.float32)

    # Physically motivated extras for hierarchical experiments:
    # band-occupancy fractions and per-band rise/decline slope proxies.
    n_safe = max(1.0, n_obs)
    frac_g = float(counts[0].item() / n_safe)
    frac_r = float(counts[1].item() / n_safe)
    frac_i = float(counts[2].item() / n_safe)
    slope_g = _safe_slope(t[band_id == 0], logf[band_id == 0])
    slope_r = _safe_slope(t[band_id == 1], logf[band_id == 1])
    slope_i = _safe_slope(t[band_id == 2], logf[band_id == 2])
    color_gr_slope = slope_g - slope_r
    color_ri_slope = slope_r - slope_i

    physics = enhanced + [
        frac_g,
        frac_r,
        frac_i,
        slope_g,
        slope_r,
        slope_i,
        color_gr_slope,
        color_ri_slope,
    ]
    return torch.tensor(physics, dtype=torch.float32)
