# draw.io with Step Functions
The [draw.io](https://github.com/jgraph/drawio) project already has a [Dockerfile](https://github.com/jgraph/drawio/blob/master/etc/docker/Dockerfile), however that builds from source which seems overkill for our needs, especially when there are packaged releases of the war file at https://github.com/jgraph/drawio/releases.

The Dockerfile in this repository takes a rather different approach:
```
FROM tomcat:9.0-slim

ENV DRAW_VERSION v12.9.5
ENV STEP_FUNCTIONS_PLUGIN_VERSION 0.6.2

RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
  apt-get install -y --no-install-recommends \
  curl && \
  # Change permission so Tomcat can start as non-root user
  chmod 777 /usr/local/tomcat/conf && \
  # Install draw.io
  cd /usr/local/tomcat/webapps/ROOT && rm -rf * && \
  curl -O -sSL https://github.com/jgraph/drawio/releases/download/${DRAW_VERSION}/draw.war && \
  jar xvf draw.war && rm draw.war && \
  # Install the step-functions-draw.io plugin
  cd plugins && \
  curl -O -sSL https://cdn.jsdelivr.net/gh/sakazuki/step-functions-draw.io@${STEP_FUNCTIONS_PLUGIN_VERSION}/dist/aws-step-functions.js && \
  # Hack/patch the draw.io App.pluginRegistry in
  # /js/app.min.js to add the aws-step-functions plugin to
  # the registry, so that it can be run with &p=sfn as per 
  # https://desk.draw.io/support/solutions/articles/16000056430-what-plugins-are-available-
  # The patch method is described in this article.
  # https://groups.google.com/forum/#!topic/drawio/ZFwk6hWGs74
  sed -i 's/"\/plugins\/tags.js"/"\/plugins\/tags.js",sfn:"\/plugins\/aws-step-functions.js"/g' /usr/local/tomcat/webapps/ROOT/js/app.min.js && \
  sed -i 's/var mxIsElectron/var plugins = "\?p=sfn";\n\t\tif (window.location.search !== plugins) {window.location.replace("index.html"+plugins);}\n\t\tvar mxIsElectron/g' /usr/local/tomcat/webapps/ROOT/index.html && \
  # Tidy up
  apt-get clean && \
  apt-get purge -y curl && \
  apt-get autoremove -y && \
  rm -rf /var/lib/apt/lists/*

USER 1001
CMD ["catalina.sh", "run"]
```
This Dockerfile is based on the official tomcat:9.0-slim image from Docker Hub, to which we also add curl so that we can install the latest versions of draw.io and the aws-step-functions plugin from https://github.com/sakazuki/step-functions-draw.io.

We next change the permissions of /usr/local/tomcat/conf, so that we can run Tomcat as a non-root user.

Next, we install draw.io by downloading the draw.war from draw.io releases https://github.com/jgraph/drawio/releases and unpacking. If we just needed draw.io *without* the plugin an alternative, and slightly simpler, approach would be to simply ADD the draw.war as ROOT.war into a tomcat container, however we need the **unpacked** war for the next step.

To install the step-functions-draw.io plugin and get it to run *automatically* has a few complications, as plugins are not part of the core functionality of draw.io. There are however a range of default plugins available in the draw.io /plugins directory, so our first step is to install aws-step-functions.js there via curl.

It is possible to load *official* draw.io plugins by using the the **p=xxxx** URL parameter as described in the draw.io documentation
https://desk.draw.io/support/solutions/articles/16000056430-what-plugins-are-available-.

Unfortunately aws-step-functions.js is not an official plugin, so we need to do yet more work. The "trick" is to add this plugin to the App.pluginRegistry as described in this article https://groups.google.com/forum/#!topic/drawio/ZFwk6hWGs74. To do this unfortunately we must "patch" the draw.io app.min.js.
```
sed -i 's/"\/plugins\/tags.js"/"\/plugins\/tags.js",sfn:"\/plugins\/aws-step-functions.js"/g' /usr/local/tomcat/webapps/ROOT/js/app.min.js
```
This simply does a *find and replace* on the last item in App.pluginRegistry "/plugins/tags.js", which we replace with "/plugins/tags.js",sfn:"/plugins/aws-step-functions.js". This registers the plugin with the name sfn.

This patch would allow us to start draw.io and launch the plugin by navigating to http://localhost:8888?p=sfn

It's all too easy to forget the query parameter however, so we next patch index.html to automatically redirect appending ?p=sfn to the original URL.
```
sed -i 's/var mxIsElectron/var plugins = "\?p=sfn";\n\t\tif (window.location.search !== plugins) {window.location.replace("index.html"+plugins);}\n\t\tvar mxIsElectron/g' /usr/local/tomcat/webapps/ROOT/index.html
```
This line simply inserts the following JavaScript code into index.html
```
var plugins = "?p=sfn";
if (window.location.search !== plugins) {
  window.location.replace("index.html"+plugins);
}
```
This forces a redirect with ?p=sfn automatically appended

This allows us to start draw.io and launch the plugin simply by navigating to http://localhost:8888

To build the image:
```
docker build -t draw .
```
To run the container:
```
docker run --rm -p 8888:8080 draw
```
or use the [drawio.sh](https://github.com/fadams/local-step-functions/blob/master/drawio/drawio.sh) script in this repository.

### Step Functions Plugin
We're using the Step Functions Plugin found at https://github.com/sakazuki/step-functions-draw.io.

To manually load the plugin:

1. Select Menu [Extras]-[Plugins]
2. Click [Add]
3. https://cdn.jsdelivr.net/gh/sakazuki/step-functions-draw.io@0.6.2/dist/aws-step-functions.js
4. [Apply]
5. Reload the page
