import matplotlib.pyplot as plt
from matplotlib import cm, colors
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

#|%%--%%| <H2FWoam1T6|xWhKwoq21t>

r = 1
pi = np.pi
cos = np.cos
sin = np.sin
phi, theta = np.mgrid[0.0:pi:100j, 0.0:2.0*pi:100j]
x = r*sin(phi)*cos(theta)
y = r*sin(phi)*sin(theta)
z = r*cos(phi)

#|%%--%%| <xWhKwoq21t|ZLr94M0zi9>

lon = 20 * pi / 180
lat = 30 * pi / 180

xx = cos(lat)*sin(lon)
yy = sin(lat)
zz = cos(lat)*cos(lon)

cartx = np.full(100, cos(lat) * sin(lon))
carty = np.full(100, sin(lat))
cartz = np.linspace(2,-2, 100)

if carty[0] == 0:
    projected_angle = 0
else:
    projected_angle = np.arctan(carty[0]/cartx[0])

angle_line_x = np.linspace(0,2) * np.cos(projected_angle)
angle_line_y = np.linspace(0,2) * np.sin(projected_angle)
angle_line_z = 0 

cme_line_x = np.linspace(0,2) * np.cos(20 * pi / 180)
cme_line_y = np.linspace(0,2) * np.sin(20 * pi / 180)
cme_line_z = 0 

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

ax.plot_surface(
    x, y, z,  rstride=1, cstride=1, color='c', alpha=0.4, linewidth=0)

ax.scatter(xx,yy,zz,color="k",s=40)
ax.plot(cartx, carty, cartz, color="g", linewidth=2)

ax.plot(angle_line_x, angle_line_y, angle_line_z, color="r", linewidth=2)
ax.plot(cme_line_x, cme_line_y, cme_line_z, color="orange", linewidth=2)

ax.view_init(90,-90)

ax.set_xlim(-1.5, 1.5)
ax.set_ylim(-1.5, 1.5)
ax.set_zlim(-1.5, 1.5)

ax.set_box_aspect([1,1,1])
