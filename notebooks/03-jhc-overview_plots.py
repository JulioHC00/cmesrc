
import pandas as pd
import matplotlib.pyplot as plt
from os import walk
import numpy as np
from tqdm import tqdm
from astropy.time import Time
from os.path import join
from matplotlib.patches import Wedge, Rectangle
from src.cmesrc.config import SWAN_DATA_DIR, RAW_DIMMINGS_CATALOGUE, PLOTTING_DATABASE_PICKLE, OVERVIEW_FIGURES_DIR
from src.cmesrc.utils import get_closest_harps_timestamp
from src.harps.harps import Harps
from src.cmes.cmes import CME
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.coordinates import Angle
from sunpy.coordinates import frames
from src.dimmings.dimmings import Dimming
import sunpy.map


#|%%--%%| <oDsWDOi3A9|mfz9tKX7rF>

def cacheSwanData() -> dict:
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t", na_values="None", usecols=['Timestamp', 'LAT_MIN', 'LON_MIN', 'LAT_MAX', 'LON_MAX']).dropna()

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    return data_dict

SWAN_DATA = cacheSwanData()

#|%%--%%| <mfz9tKX7rF|WhTajwwSLc>

plotting_data = pd.read_pickle(PLOTTING_DATABASE_PICKLE)
plotting_data.set_index("cme_id", drop=False, inplace=True)

plotting_mask = plotting_data["cme_date"] < Time("2019-01-07", format="iso")

dimmings_data = pd.read_csv(RAW_DIMMINGS_CATALOGUE)
dimmings_data["dimming_time"] = Time(dimmings_data["max_detection_time"].to_list(), format="iso")
dimmings_data.set_index("dimming_id", drop=False, inplace=True)
dimmings_data["off_disk"] = dimmings_data["longitude"].isna()

#|%%--%%| <WhTajwwSLc|EF3HcZV3Xn>

def plot_cme(ax, sunpy_map, principal_angle, angular_width, halo = False, distance=0.95, npoints=100, linestyle="solid", color="green", linewidth=1, alpha=1):
    center = sunpy_map.world_to_pixel(sunpy_map.center)
    rad = min(center) * distance

    if halo:
        principal_angle = Angle('0d')
        angular_width = Angle('359.99d')
    else:
        principal_angle = Angle(f'{principal_angle}d')
        angular_width = Angle(f'{angular_width}d')

    ninety_deg = Angle('90d')

    lower_angle = (principal_angle + ninety_deg - angular_width / 2).to(u.rad)
    upper_angle = (principal_angle + ninety_deg + angular_width / 2).to(u.rad)

    angles = np.linspace(lower_angle, upper_angle, npoints)

    modified_x = []
    modified_y = []

    for angle in angles:
        modified_x.append((rad * np.cos(angle)).value)
        modified_y.append((rad * np.sin(angle)).value)

    if not halo:
        modified_x.insert(0,0)
        modified_y.insert(0,0)
        modified_x.append(0)
        modified_y.append(0)

    modified_x = np.array(modified_x) * u.pix
    modified_y = np.array(modified_y) * u.pix

    real_x = modified_x + center[0]
    real_y = modified_y + center[1]

    points = SkyCoord(np.array(list(map(sunpy_map.pixel_to_world, real_x, real_y))))

    ax.plot_coord(points, c=color, linewidth=linewidth, linestyle=linestyle, label=f"PA={principal_angle}, WIDTH={angular_width}", zorder=20, alpha=alpha)
    return ax

def plot_off_disk_dimming(ax, sunpy_map, x, y, dimming_id, color="#D73F28"):
    center = sunpy_map.world_to_pixel(sunpy_map.center)
    rad = min(center).value

    modified_x = rad * x * u.pix
    modified_y = rad * y * u.pix
    text_y = modified_y - 10 * u.pix

    real_x = modified_x + center[0]
    real_y = modified_y + center[1]
    real_text_y = text_y + center[1]


    point = SkyCoord(sunpy_map.pixel_to_world(real_x, real_y))
    text_point = SkyCoord(sunpy_map.pixel_to_world(real_x, real_y))

    ax.plot_coord(point, zorder=10, marker="x", c=color)

N = 100

for cme_id, data in tqdm(plotting_data[plotting_mask].sample(N).iterrows(), total=N):

    cme_time = data["cme_date"]
    cme_pa = data["cme_pa"]
    cme_width = data["cme_width"]
    cme_halo = data["cme_halo"]

    cme = CME(cme_time, cme_pa, cme_width, halo=cme_halo)

    observer = frames.HeliographicStonyhurst(0 * u.rad, 0 * u.rad, radius= 1 * u.AU)

    header_data = np.full((10, 10), np.nan)

    ref_coord = SkyCoord(0*u.arcsec, 0*u.arcsec, obstime=cme_time,
                    observer=observer, frame=frames.Helioprojective)

    header = sunpy.map.make_fitswcs_header(header_data, ref_coord, scale=[220, 220]*u.arcsec/u.pixel)

    blank_map = sunpy.map.Map(header_data, header)
#
    fig = plt.figure(figsize=(8,8), dpi=200)

    ax = fig.add_subplot(projection=blank_map)

    blank_map.plot(axes=ax)
    blank_map.draw_limb(axes=ax, color="k")
    blank_map.draw_grid(axes=ax, color="k", zorder=-1, alpha=0.2)
#
    plot_cme(ax, blank_map, cme_pa, cme_width, distance=1.1, npoints=100, halo=cme_halo)

    if not cme_halo:
        plot_cme(ax, blank_map, cme_pa, cme_width + cme.WIDTH_EXTRA_ANGLE, distance=1.1, npoints=100, linewidth=1, linestyle="dotted", alpha=0.5)

    all_harps = data["all_harps"]

    lat_text = np.linspace(-45, 45, len(all_harps))
    transparent_white = (1, 1, 1, 0.4)

    plotted_harps_dict = dict()

    for i, harpsnum in enumerate(data["non_spatial_harps"]):
        harps_timestamps = SWAN_DATA[harpsnum]["Timestamp"].to_list()
        closest_timestamp = get_closest_harps_timestamp(harps_timestamps, cme_time)

        harps_data = SWAN_DATA[harpsnum].loc[closest_timestamp][["Timestamp","LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]]

        harps = Harps(*harps_data)
        rotated_harps = harps.rotate_bbox(cme_time)

        plotted_harps_dict[harpsnum] = rotated_harps

        blank_map.draw_quadrangle(
                bottom_left=rotated_harps.LOWER_LEFT.get_skycoord(),
                top_right=rotated_harps.UPPER_RIGHT.get_skycoord(),
                axes=ax,
                zorder=10,
                edgecolor="#D73F28",
                )

        ax.annotate(harpsnum, 
                    (rotated_harps.get_centre_point().LON, rotated_harps.get_centre_point().LAT),
                    xytext=(rotated_harps.get_centre_point().LON, rotated_harps.LOWER_LEFT.LAT-5),
                    xycoords=ax.get_transform('heliographic_stonyhurst'),
                    backgroundcolor=transparent_white,
                    color='k',
                    horizontalalignment='center', 
                    verticalalignment='top',
                    fontsize=6,
                    zorder=30
                    )


        ax.plot_coord(rotated_harps.get_centre_point().get_skycoord(), c="k", zorder=10, marker=".", markersize=1)

    for i, harpsnum in enumerate(data["spatial_harps"]):
        harps_timestamps = SWAN_DATA[harpsnum]["Timestamp"].to_list()
        closest_timestamp = get_closest_harps_timestamp(harps_timestamps, cme_time)

        harps_data = SWAN_DATA[harpsnum].loc[closest_timestamp][["Timestamp","LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]]

        harps = Harps(*harps_data)
        rotated_harps = harps.rotate_bbox(cme_time)

        plotted_harps_dict[harpsnum] = rotated_harps

        blank_map.draw_quadrangle(
                bottom_left=rotated_harps.LOWER_LEFT.get_skycoord(),
                top_right=rotated_harps.UPPER_RIGHT.get_skycoord(),
                axes=ax,
                zorder=10,
                edgecolor="#28C0D7",
                )

        ax.annotate(harpsnum, 
                    (rotated_harps.get_centre_point().LON, rotated_harps.get_centre_point().LAT),
                    xytext=(rotated_harps.get_centre_point().LON, rotated_harps.LOWER_LEFT.LAT-5),
                    xycoords=ax.get_transform('heliographic_stonyhurst'),
                    backgroundcolor=transparent_white,
                    color='k',
                    horizontalalignment='center', 
                    verticalalignment='top',
                    fontsize=6,
                    zorder=30
                    )


        ax.plot_coord(rotated_harps.get_centre_point().get_skycoord(), c="k", zorder=10, marker=".", markersize=1)

    for dimming_id in data["non_spatial_dimmings"]:
        dimming_data = dimmings_data.loc[dimming_id]

        if dimming_data["off_disk"]:
            plot_off_disk_dimming(ax, blank_map, dimming_data["avg_x"], dimming_data["avg_y"], dimming_id)
        else:
            dimming = Dimming(
                    date = dimming_data["dimming_time"],
                    lon = dimming_data["longitude"],
                    lat = dimming_data["latitude"]
                    )
            ax.plot_coord(dimming.point.rotate_coords(cme_time).get_skycoord(), c="#D73F28", zorder=10, marker="x")

            ax.annotate(f"D{dimming_id}", 
                        (0, 0),
                        xytext=(dimming.point.LON, dimming.point.LAT + 5),
                        xycoords=ax.get_transform('heliographic_stonyhurst'),
                        backgroundcolor=transparent_white,
                        color='k',
                        horizontalalignment='center', 
                        verticalalignment='top',
                        fontsize=6,
                        zorder=30
                        )

    spatial_dimmings = data["spatial_dimmings"]
    for dimming_id in spatial_dimmings.keys():
        dimming_data = dimmings_data.loc[dimming_id]

        if dimming_data["off_disk"]:
            plot_off_disk_dimming(ax, blank_map, dimming_data["avg_x"], dimming_data["avg_y"], dimming_id, color="#28C0D7")
        else:
            dimming = Dimming(
                    date = dimming_data["dimming_time"],
                    lon = dimming_data["longitude"],
                    lat = dimming_data["latitude"]
                    )

            dimming_skycoord = dimming.point.rotate_coords(cme_time).get_skycoord()
            harps_centre = plotted_harps_dict[spatial_dimmings[dimming_id]].get_centre_point().get_skycoord()

            line = SkyCoord([dimming_skycoord, harps_centre])
            ax.plot_coord(dimming_skycoord, c="#28C0D7", zorder=10, marker="x")

            ax.plot_coord(line, c="k", linewidth=1, zorder=40)

            ax.annotate(f"D{dimming_id}", 
                        (0, 0),
                        xytext=(dimming.point.LON, dimming.point.LAT + 5),
                        xycoords=ax.get_transform('heliographic_stonyhurst'),
                        backgroundcolor=transparent_white,
                        color='k',
                        horizontalalignment='center', 
                        verticalalignment='top',
                        fontsize=6,
                        zorder=30
                        )

    plt.savefig(OVERVIEW_FIGURES_DIR + str(cme_id) + ".png", dpi=150, facecolor="#555555")
    plt.clf()
    plt.cla()
    plt.close()
