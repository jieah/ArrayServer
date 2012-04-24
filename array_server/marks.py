
import numpy as np
import time
from enthought.traits.api import HasTraits, Any, on_trait_change, Int, Float, Property, Enum
from enthought.enable.api import MarkerTrait, MarkerNameDict, Component, LineStyle, ColorTrait
from enthought.chaco.scatterplot import render_markers

# Local imports
import npproxy as npp
from grapheval import GraphNode


def get_array_gen_or_scalar(self, attrname):
    return getattr(self, "_"+attrname)
def set_array_gen_or_scalar(self, attrname, new):
    if not isinstance(getattr(self, "_"+attrname), GraphNode):
        setattr(self, "_"+attrname, new)
        self.trait_property_changed(attrname, new)
    else:
        raise NotImplementedError
ArrayGenOrScalar = Property(get_array_gen_or_scalar, set_array_gen_or_scalar)


class Mark(Component):

    xs = Property()
    ys = Property()

    # the follow are ints, floats, or arrays of the same length as points
    size = ArrayGenOrScalar()
    angle = ArrayGenOrScalar()
    color = ColorTrait
    outline_color = ColorTrait

    orientation = Enum("horizontal", "vertical")

    marker = MarkerTrait

    _x = Any()
    _y = Any()
    _size = Int(5)
    _angle = Float(0)

    @classmethod
    def from_xy(cls, x, y, **kwtraits):
        kwtraits.update({"_x":x, "_y":y})
        return cls(**kwtraits)

    def _get_xs(self):
        return self._x

    def _get_ys(self):
        return self._y

    def _draw_plot(self, gc, mode="normal", view_bounds=None):
        t1 = time.time()
        pts = np.column_stack((self._x, self._y))
        t2 = time.time()
        print "compute time:", t2 - t1
        render_markers(gc, pts, self.marker, self.size, self.color_, 1, self.outline_color_)
        print "render time:", time.time() - t2


class Square(Mark):
    marker = "square"

class Diamond(Mark):
    marker = "diamond"

class Circle(Mark):
    marker = "circle"

class Triangle(Mark):
    marker = "triangle"


class Line(Component):
    xs = Property()
    ys = Property()

    size = Float(1)
    style = LineStyle()

    _x = Any()
    _y = Any()

    @classmethod
    def from_xy(cls, x, y, **kwtraits):
        kwtraits.update({"_x":x, "_y":y})
        return cls(**kwtraits)

    def _get_xs(self):
        return self._x

    def _get_ys(self):
        return self._y

    def _draw_plot(self, gc, mode="normal", view_bounds=None):
        pts = np.column_stack((self._x, self._y))
        with gc:
            gc.set_stroke_color((1,0,0,1))
            gc.set_line_width(self.size)
            gc.set_line_dash(self.style_)
            gc.begin_path()
            gc.lines(pts)
            gc.stroke_path()


class Wedge(Component):

    @property
    def r1(self):
        return self._r1

    @property
    def r2(self):
        return self._r2

    @property
    def theta1(self):
        return self._theta1

    @property
    def theta2(self):
        return self._theta2

    size = Float(1)
    style = LineStyle()
    color = ColorTrait
    outline_color = ColorTrait

    _r1 = Any()
    _r2 = Any()
    _theta1 = Any()
    _theta2 = Any()

    @classmethod
    def from_rtheta(cls, rs, thetas=None, **kwtraits):
        """ Non-uniform r and theta positions, but each connected to the previous
        """
        obj = cls(**kwtraits)
        obj._r1 = rs
        if thetas is None:
            obj._theta1 = np.linspace(0, 2*np.pi, len(rs))
        else:
            obj._theta1 = thetas
        return obj

    @classmethod
    def from_patches(cls, r1, r2, t1, t2, **kwtraits):
        """ Gives total control over the patches, defining the start/end radii
        and angles for each wedge
        """
        obj = cls(**kwtraits)
        obj._r1 = r1
        obj._r2 = r2
        obj._theta1 = t1
        obj._theta2 = t2
        return obj

    def _draw_plot(self, gc, mode="normal", view_bounds=None):
        # Fundamentally we have to determine if we are doing a line
        # plot or area plot
        if self._r2 is None:
            self._render_lines(gc)
        else:
            self._render_areas(gc)
            
    def _render_lines(self, gc):
        pts = np.column_stack((
            self._r1 * np.cos(self._theta1) + self.width/2,
            self._r1 * np.sin(self._theta1) + self.height/2
            ))
        with gc:
            gc.set_stroke_color(self.outline_color_)
            gc.set_line_width(self.size)
            gc.begin_path()
            gc.lines(pts)
            gc.stroke_path()

    def _render_areas(self, gc):
        # In the following, we compute the four points defining the circular
        # wedge.  Point 1 is the min radius and min theta

        t1 = self._theta1
        t2 = self._theta2
        t1c = np.cos(self._theta1)
        t1s = np.sin(self._theta1)
        x1 = self._r1 * t1c
        y1 = self._r1 * t1s
        x2 = self._r2 * t1c
        y2 = self._r2 * t1s
        x3 = self._r2 * np.cos(self._theta2)
        y3 = self._r2 * np.sin(self._theta2)
        x4 = self._r1 * np.cos(self._theta2)
        y4 = self._r1 * np.sin(self._theta2)

        # center of the circle
        xc = self.width / 2
        yc = self.height / 2

        with gc:
            gc.set_stroke_color(self.outline_color_)
            gc.set_fill_color(self.color_)
            gc.set_line_width(self.size)
            for i in xrange(len(x1)):
                # Because the lines() method starts with a move_to, it causes
                # a break in the polygon.  So we have to build up the overall
                # polygon of the wedge, and then draw it all at once with a 
                # single call to lines()

                gc.begin_path()
                arcx1, arcy1 = self._arc(gc, self._r1[i], t1[i], t2[i])
                arcx2, arcy2 = self._arc(gc, self._r2[i], t2[i], t1[i])
                xs = np.hstack((x1[i], arcx1, x3[i], arcx2, x2[i]))
                ys = np.hstack((y1[i], arcy1, y3[i], arcy2, y2[i]))
                gc.lines(np.column_stack((xs+xc, ys+yc)))
                gc.close_path()
                gc.draw_path()

    def _arc(self, gc, radius, t1, t2):
        arclen = abs(radius * (t2 - t1))
        numpts = max(int(arclen / 4),2)     # 4 pixels is longest polygon)
        angles = np.linspace(t1, t2, numpts)
        xs = radius*np.cos(angles)
        ys = radius*np.sin(angles)
        return (xs[1:], ys[1:])

    #@r1.setter
    #def r1(self, value):
    #    pass

    #@r2.setter
    #def r2(self, value):
    #    pass

    #@theta1.setter
    #def theta1(self, value):
    #    pass

    #@theta2.setter
    #def theta2(self, value):
    #    pass

