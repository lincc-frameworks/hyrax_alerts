from collections import Counter

import matplotlib.pyplot as plt


def calculate_phot_flag(flux, flux_err):
    return 4069 if flux / flux_err > 5 else 0


def add_phot_flag(self, batch):
    for sample in batch:
        alert = sample.get("alert")
        if alert is not None:
            for dia_source in alert.get("diaSourcesList", []):
                dia_source["photFlag"] = calculate_phot_flag(dia_source["psfFlux"], dia_source["psfFluxErr"])
            for dia_forced_source in alert.get("diaForcedSourcesList", []):
                dia_forced_source["photFlag"] = calculate_phot_flag(
                    dia_forced_source["psfFlux"], dia_forced_source["psfFluxErr"]
                )
    return batch


def lasair_pre_process(self, batch):
    """Basic printing for "pre-processing"

    Parameters
    ----------
    self : object
        The instance of the class calling this function.
    batch : list
        A batch of data to be pre-filtered.

    Returns
    -------
    list
        The pre-filtered batch of data, unchanged from the input.
    """
    print("Example pre-filtering function called.")
    for i, sample in enumerate(batch):
        print(f"Sample {i}: {sample}")
        alert = sample.get("alert")
        if alert is None:
            print("---diaSourcesList not found---")
        else:
            print(f"diaSourcesList - {len(alert.get('diaSourcesList', []))} entries")
            if alert.get("diaSourcesList"):
                print(f"  {alert['diaSourcesList'][0]}")
            # for i in alert["diaSourcesList"][:1]:
            #     print(f"  {i}")

            print(f"diaForcedSourcesList - {len(alert.get('diaForcedSourcesList', []))} entries")
            if alert.get("diaForcedSourcesList"):
                print(f"  {alert['diaForcedSourcesList'][0]}")
            # for i in alert["diaForcedSourcesList"][:5]:
            #     print(f"  {i}")

            if i % 50 == 0:
                do_plot(sample)

    return batch


def do_plot(sample):
    alert = sample.get("alert")

    num_sources = len(alert["diaSourcesList"])
    flux = [alert["diaSourcesList"][s]["psfFlux"] for s in range(num_sources)]
    dflux = [alert["diaSourcesList"][s]["psfFluxErr"] for s in range(num_sources)]
    time = [alert["diaSourcesList"][s]["midpointMjdTai"] for s in range(num_sources)]
    band = [alert["diaSourcesList"][s]["band"] for s in range(num_sources)]
    count = len(flux)

    num_forced_sources = len(alert["diaForcedSourcesList"])
    fflux = [alert["diaForcedSourcesList"][s]["psfFlux"] for s in range(num_forced_sources)]
    fdflux = [alert["diaForcedSourcesList"][s]["psfFluxErr"] for s in range(num_forced_sources)]
    ftime = [alert["diaForcedSourcesList"][s]["midpointMjdTai"] for s in range(num_forced_sources)]
    fband = [alert["diaForcedSourcesList"][s]["band"] for s in range(num_forced_sources)]
    fcount = len(fflux)

    # Create the figure and axes
    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot PSF Flux vs Time
    # ax.scatter(time, flux, color='tab:blue', label='PSF Flux', alpha=0.8)
    ax.errorbar(time, flux, yerr=dflux, fmt="o", color="tab:blue", label=f"PSF Flux ({count})", alpha=0.8)

    # Plot PSF Difference Flux vs Time on the same axes
    # ax.scatter(time, dflux, color='tab:orange', marker='s', label='PSF Diff Flux', alpha=0.8)

    # Plot Forced PSF Flux vs Time
    # ax.scatter(ftime, fflux, color='tab:green', marker='^', label='Forced PSF Flux', alpha=0.8)
    ax.errorbar(
        ftime, fflux, yerr=fdflux, fmt="^", color="tab:green", label=f"Forced PSF Flux ({fcount})", alpha=0.8
    )

    # Plot Forced PSF Difference Flux vs Time on the same axes
    # ax.scatter(ftime, fdflux, color='tab:red', marker='v', label='Forced PSF Diff Flux', alpha=0.8)

    # Add a dashed reference line at zero (helpful for difference flux)
    ax.axhline(0, color="gray", linestyle="--", linewidth=1, alpha=0.5)

    band_counter = Counter(band)
    fband_counter = Counter(fband)
    # Labels, title, and legend
    ax.set_xlabel("Time (MJD TAI)")
    ax.set_ylabel("Flux")
    ax.set_title(
        f'Object Source Light Curve (Object id: {sample["id"]})\nBands: {band_counter}\nForced Bands: {fband_counter}'
    )
    ax.legend()

    plt.tight_layout()
    plt.show()
