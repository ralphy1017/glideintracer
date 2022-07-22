
# GlideinWMS documentation for GlideFactoryLib.py, constructing 
# a Tracer and Trace class to give Glidein's unique TRACE_ID 
# and methods to send child spans or print the TRACE_ID of Glidein

import jaeger_client
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


# Global variables to establish tracing to Jaeger
# ideally, these are read from the environment and have defaults (ex: server = "localhost", port = 6831)
jaeger_service = "glidein"
server = "fermicloud296.fnal.gov"
port = "16686"


class Tracer:
    """ Initializes a tracer with OpenTelemetry and Jaeger when operated in the GlideinWMS Factory

    Retrives environment variables to identify which Jaeger server and port to send to. 
   
    Args:
        self: an instance of a new tracer for traces to send through
        server: str ## if not "localhost", then the host where the Jaeger UI is running
        port: int ## the port to which traces will talk to Jaeger

    Returns:
        a new tracer instance when the method 'self.initial_trace' is used, initializes a new parent 
        trace (ideally for each new glidein) stored as GLIDEIN_TRACE_ID
    """

    def __init__(self, server, port):
        self.server = server
        self.port = port
        # Variables for each glidein instance to be used for Trace class methods
        self.GLIDEIN_TRACE_ID = None
        self.tracer = None

    def initial_trace(self):
        try:
            config={
                "sampler": {
                    "type": "const",
                    "param": 1,
                },
                'local_agent': {
                    'reporting_host': self.server,
                    'reporting_port': self.port,
                },
                'logging': True
            }
            self.tracer=jaeger_client.Config(config=config,service_name=jaeger_service).initialize_tracer()
            self.span=self.tracer.start_span("parent")
            data={}
            self.tracer.inject(self.span.context, "text_map", data)
            self.GLIDEIN_TRACE_ID = (data["uber-trace-id"])
            self.span.finish()
            return 0
        except:
            print("PARENT TRACE INITIALIZE ERROR")
            return 1
        
        
        
class Trace:
    """ Initializes tracing with an established tracer, with send_span and get_trace_ID methods
    
    Args:
        trace: initialized trace from Tracer class
        GLIDEIN_TRACE_ID: unique trace_id (ideally unique for each glidein instance)
        
    Returns:
        send_span: a child span of the parent (same trace_ID)
            # THIS WOULD BE DIFFERENT pfeil COMMANDS EXECUTED IN TERMINAL
            
        get_trace_ID: the glidein's trace id as output and return value
    """
    def __init__(self, trace, TRACE_ID): 
        self.trace = trace
        self.TRACE_ID = TRACE_ID

    def send_span(self):
        try:
            with self.trace.start_span("child"):
                
                print("CHILD SPAN SENT")
                return 0
        except:
            print("CHILD SPAN SEND ERROR")
            return 1
    
    def get_trace_ID(self):
        print(self.TRACE_ID)
        return(self.TRACE_ID)



def main(): # use classes above to initialize a tracer and send a span and print trace id
    T = Tracer(server,port)
    T.initial_trace()
    t = Trace(T.tracer,T.GLIDEIN_TRACE_ID)
    t.send_span()
    t.get_trace_ID()



if __name__ == "__main__":
    try:
        main()
    except:
        print("ERROR main()")
