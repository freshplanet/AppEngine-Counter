import webapp2

app = webapp2.WSGIApplication([
    webapp2.Route('/',                             'counter.views.SampleHandler'),
    webapp2.Route('/admin/counters/',              'counter.adminViews.CountersQueryHandler'),
    webapp2.Route('/admin/counters/chart/',        'counter.adminViews.CountersChartHandler'),
])
