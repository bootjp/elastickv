package clusterbootstrap

import (
	"slices"

	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
)

// These methods bypass session framing only. Their ordinary authentication and
// revocable runtime admission remain mandatory. Unknown methods stay denied.
func BootstrapPublicGRPCMethods() []string {
	mixed := make(map[string]bool)
	for _, method := range BootstrapSharedGRPCMethods(0) {
		mixed[method.Name] = true
	}
	private := map[string]bool{
		pb.Internal_IssueBoundTimestamp_FullMethodName:    true,
		pb.Internal_LookupTimestampBinding_FullMethodName: true,
	}
	var methods []string
	for _, service := range []*grpc.ServiceDesc{
		&pb.RawKV_ServiceDesc, &pb.TransactionalKV_ServiceDesc, &pb.Internal_ServiceDesc,
		&pb.Distribution_ServiceDesc, &pb.Admin_ServiceDesc, &pb.AdminForward_ServiceDesc,
		&pb.EncryptionAdmin_ServiceDesc, &pb.RaftAdmin_ServiceDesc, &pb.S3BlobFetch_ServiceDesc,
	} {
		for _, method := range service.Methods {
			name := "/" + service.ServiceName + "/" + method.MethodName
			if !mixed[name] && !private[name] {
				methods = append(methods, name)
			}
		}
		for _, stream := range service.Streams {
			methods = append(methods, "/"+service.ServiceName+"/"+stream.StreamName)
		}
	}
	methods = append(methods, "/grpc.health.v1.Health/Check", "/grpc.health.v1.Health/Watch",
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo")
	slices.Sort(methods)
	return methods
}
