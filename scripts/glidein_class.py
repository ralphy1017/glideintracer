
# GlideinWMS documentation for GlideFactoryLib.py, constructing 
# a Tracer and Trace class to give Glidein's unique TRACE_ID 
# and methods to send child spans or print the TRACE_ID of Glidein

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator


# Global variables to establish tracing to Jaeger
# ideally, these are read from the environment and have defaults (ex: server = "localhost", port = 6831)
jaeger_service = "glidein"
server = "localhost"
port = 6831


class Tracer:
    def __init__(self, server, port):
        """Initializes a tracer with OpenTelemetry and Jaeger when operated in the GlideinWMS Factory

        Args:
            server (str): if not "localhost", then the host where the Jaeger Agent is running in a container
            port (int): the port to which traces will talk to Jaeger
            
        Variables:
            GLIDEIN_TRACE_ID (hex): the parent trace_id of each submitted glidein instance
            tracer: a tracer instance for each glidein to generate more spans
        """
        self.server = server
        self.port = port
        self.GLIDEIN_TRACE_ID = None
        self.tracer = None
        self.carrier = None

    def initial_trace(self):
        trace.set_tracer_provider(
        TracerProvider(
                resource=Resource.create({SERVICE_NAME:jaeger_service})
            )
        )
        
        self.tracer = trace.get_tracer(__name__)
        
        jaeger_exporter = JaegerExporter(
            agent_host_name=self.server,
            agent_port=self.port,
        )
        span_processor = BatchSpanProcessor(jaeger_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)
        
        self.carrier = {} #used to propogate spanContext to child spans
        
        with self.tracer.start_as_current_span("parent") as parent: #this is the parent span for each submitted glidein
            TraceContextTextMapPropagator().inject(carrier=self.carrier)
            self.SpanContext = parent.get_span_context()
            self.GLIDEIN_TRACE_ID = (hex(self.SpanContext.trace_id))
            
class Trace:
    """ Initializes tracing with an established tracer, with send_span and get_trace_ID methods
    
    Args:
        trace: initialized trace from Tracer class
        GLIDEIN_TRACE_ID: unique trace_id (ideally unique for each glidein)
        
    Returns:
        send_span: a child span of the parent (same trace_ID)
            # THIS WOULD BE DIFFERENT pfeil COMMANDS EXECUTED IN TERMINAL
            
        get_trace_ID: the glidein's trace id as output and return value
    """
    def __init__(self, tracer, carrier): 
        """Initializes tracing with an established tracer, with send_span and get_span_ID methods

        Args:
            tracer (): _description_
            GLIDEIN_TRACE_ID (_type_): _description_
        """
        self.tracer = tracer
        self.carrier = carrier
        self.GLIDEIN_SPAN_ID = None
        self.SpanContext = None
        self.ctx = None
        

    def send_span(self):
        self.ctx = TraceContextTextMapPropagator().extract(carrier=self.carrier)
        with self.tracer.start_as_current_span("child", context=self.ctx) as child:
            self.SpanContext = child.get_span_context()
            self.GLIDEIN_SPAN_ID = (hex(self.SpanContext.span_id))
    
    def get_span_ID(self):
        print(self.GLIDEIN_SPAN_ID)

def main(): # use classes above to initialize a tracer and send a span and print trace id
    T = Tracer(server,port)
    T.initial_trace()
    print(T.GLIDEIN_TRACE_ID)
    t = Trace(T.tracer,T.carrier)
    t.send_span()
    t.get_span_ID()

if __name__ == "__main__":
    main()
