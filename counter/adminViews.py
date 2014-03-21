# -*- coding: utf-8 -*-
'''
Copyright 2014 FreshPlanet (http://freshplanet.com | opensource@freshplanet.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import json
import os

import jinja2
import webapp2

from counter.models import Counter


class BaseHandler(webapp2.RequestHandler):
    
    jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
                               extensions=['jinja2.ext.autoescape'],
                               autoescape=True)
    
    def writeTemplate(self, fileName, context):
        """ Shortcut to render a template as a str into the request response. """
        template = self.jinja.get_template(fileName)
        self.response.write(template.render(context))
        self.response.content_type = 'text/html'


class CountersQueryHandler(BaseHandler):
    
    def get(self):
        """
        Query and Show counters row data.
        @param prefix: Counters name prefix to query for
        """
        prefix = self.request.get('prefix')
        if not prefix:
            self.abort(400, "Missing 'prefix' parameter")
        batchSize = int(self.request.get('batchSize', '50'))
        
        counters = Counter.getByPrefix(prefix, limit=batchSize).get_result()
        names = counters.keys()
        names.sort()
        data = [{'name': name, 'value': counters[name].value, 'byDay': counters[name].bySlice} for name in names]
        
        self.writeTemplate('counters.html', {'prefix': prefix, 'counters': data})


class CountersChartHandler(BaseHandler):
    
    def get(self):
        """
        Chart about specified counters
        
        @param names: comma separated list of counter names
        @param captions: (optional) comma separated list of user-readable names (should match counter names)
        @param title: (optional)
        
        Ex: "?names=email_bounce,email_unsubscribe&captions=Bounces,Unsubscribes&title=Emails Stats"
        """
        names = self.request.get('names')
        if not names:
            self.response.content_type = 'text/plain'
            self.response.write("Invalid Input.\n\n" + self.get.__doc__)
            return
        names = [n.strip() for n in names.split(',')]
        
        captions = self.request.get('captions')
        prefix = None
        if captions:
            captions = [n.strip() for n in captions.split(',')]
            if captions and len(captions) != len(names):
                self.response.content_type = 'text/plain'
                self.response.write("Invalid Input.\n\n" + self.get.__doc__)
                return
        else:
            for name in names:
                if prefix:
                    common = ''
                    for cn, cp in zip(name, prefix):
                        if cn == cp:
                            common += cn
                    prefix = common
                else:
                    prefix = name
            
            if prefix:
                captions = [name[len(prefix):] for name in names]
            else:
                captions = names
        
        title = self.request.get('title') or prefix or 'Counters Chart'
        
        counterFuts = [Counter.getCurrent(name) for name in names]
        counters = [fut.get_result() for fut in counterFuts]
        
        slices = set()
        for counter in counters:
            slices.update(counter.bySlice)
        slices = sorted(slices)
        
        chartData = [["On"] + captions]
        for item in slices:
            row = [item]
            for counter in counters:
                row.append(counter.bySlice.get(item, 0))
            chartData.append(row)
            
        self.writeTemplate("countersChart.html", {"chartData": json.dumps(chartData), "title": title})
