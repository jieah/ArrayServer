import numpy as np
from numpy import pi
from enthought.chaco.api import PlotGraphicsContext, OverlayPlotContainer

import grapheval
from array_proxy import ArrayProxy
import npproxy as npp
import marks

def draw_plot(cont, filename, size=(800,600)):
    cont.outer_bounds = list(size)
    cont.do_layout(force=True)
    gc = PlotGraphicsContext(size, dpi=72)
    import time; t1 = time.time()
    gc.render_component(cont)
    print "total render time:", time.time() - t1
    gc.save(filename)
    return


def test1():
    width = 800
    height = 600
    x = npp.linspace(-2*pi, 2*pi, 300)
    y = npp.sin(x)
    
    xs = x * width/(4*pi) + width/2.0
    ys = y * height/2 + height/2.0
    xs.flags.append("cache")
    ys.flags.append("cache")
    mask = y < 0.5
    mark = marks.Square.from_xy(xs[mask], ys[mask], color=(1,0,0,1))
    mark3 = marks.Square.from_xy(xs[~mask], ys[~mask], color=(0,1,0,1))
    line = marks.Line.from_xy(xs, ys)

    y2 = y + npp.cos(x/4)
    mark2 = marks.Triangle.from_xy(xs, y2 * height/10 + height/2.0, size=3)
    container = OverlayPlotContainer()
    container.add(mark, mark3)
    draw_plot(container, "test1.png", (width, height))

def test2():
    grapheval.DEBUG = True
    width = 800
    height = 600
    x = ArrayProxy(np.linspace(-2*pi, 2*pi, 10))
    y = npp.sin(x)
    
    xs = x * width/(4*pi) + width/2.0
    ys = y * height/2 + height/2.0
    #import pdb; pdb.set_trace()
    pts = npp.column_stack((xs, ys))
    return x, y, xs, ys, pts

def testpolar():
    width = 800
    height = 800
    theta = ArrayProxy(np.linspace(0, 2*pi, 800))
    r = 300 + (40 * npp.sin(theta*8) + 20)

    wedge = marks.Wedge.from_rtheta(r, theta, outline_color="red",
                    bounds = [width, height],
                    resizable = "")
    
    circle_r = 320 * npp.ones_like(r)
    wedge2 = marks.Wedge.from_rtheta(circle_r, outline_color=(0.4, 0.4, 0.4, 1),
                    bounds = [width, height],
                    resizable = "")

    container = OverlayPlotContainer()
    container.add(wedge, wedge2)
    draw_plot(container, "polar1.png", (width, height))

def testwedge():
    width = 800
    height = 800
    numpoints = 40

    allthetas = ArrayProxy(np.linspace(0, 2*pi, numpoints+1))
    theta1 = allthetas[:-1]
    theta2 = allthetas[1:]

    r1 = np.random.uniform(200., 240, numpoints)
    r2 = np.random.uniform(20, 50, numpoints)
    r3 = np.random.uniform(20, 70, numpoints)
    r4 = np.random.uniform(20, 50, numpoints)

    wedge = marks.Wedge.from_patches(r1, r1+r2, theta1, theta2, 
                color=(0.8, 0.1, 0.1, 1),
                bounds = [width, height],
                resizable = "")

    wedge2 = marks.Wedge.from_patches(wedge.r2, wedge.r2+r3, theta1, theta2,
                color=(0.2, 0.8, 0.2, 1),
                bounds = [width, height],
                resizable = "")

    wedge3 = marks.Wedge.from_patches(wedge2.r2, wedge2.r2+r4, theta1, theta2,
                color=(0.2, 0.2, 0.8, 1),
                bounds = [width, height],
                resizable = "")

    container = OverlayPlotContainer()
    container.add(wedge, wedge2, wedge3)
    draw_plot(container, "polar2.png", (width, height))

test1()

#x, y, xs, ys, pts = test2()
#test1()

#testpolar()

testwedge()

