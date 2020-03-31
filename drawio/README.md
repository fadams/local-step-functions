# draw.io with Step Functions
The [draw.io](https://github.com/jgraph/drawio) project already has a [Dockerfile](https://github.com/jgraph/drawio/blob/master/etc/docker/Dockerfile), however that builds from source which seems overkill for our needs, especially when there are packaged releases of the war file at https://github.com/jgraph/drawio/releases.

This alternative Dockerfile takes a slightly simpler approach, simply ADDing the draw.war as ROOT.war into a tomcat container.

```
FROM tomcat:9.0-slim

ENV DRAW_VERSION v12.9.5
ADD https://github.com/jgraph/drawio/releases/download/${DRAW_VERSION}/draw.war /usr/local/tomcat/webapps/ROOT.war

RUN rm -rf /usr/local/tomcat/webapps/ROOT && rm -rf /var/lib/apt/lists/*

CMD ["catalina.sh", "run"]
```
To build the image:
```
docker build -t draw .
```
To run the container:
```
docker run --rm -p 8888:8080 draw
```
To use, browse to: http://localhost:8888

### Step Functions Plugin
We're using the Step Functions Plugin found at https://github.com/sakazuki/step-functions-draw.io

1. Select Menu [Extras]-[Plugins]
2. Click [Add]
3. https://cdn.jsdelivr.net/gh/sakazuki/step-functions-draw.io@0.6.2/dist/aws-step-functions.js
4. [Apply]
5. Reload the page
