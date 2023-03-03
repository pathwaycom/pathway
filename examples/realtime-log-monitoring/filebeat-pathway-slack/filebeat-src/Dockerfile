FROM docker.elastic.co/beats/filebeat:8.6.1


COPY ./filebeat-src/filebeat.docker.yml /usr/share/filebeat/filebeat.yml
COPY ./filebeat-src/generate_input_stream.sh /usr/share/filebeat/generate_input_stream.sh
USER root

RUN mkdir /input_stream/
RUN touch /input_stream/example.log

RUN chown root:filebeat /usr/share/filebeat/filebeat.yml
RUN chmod go-w /usr/share/filebeat/filebeat.yml