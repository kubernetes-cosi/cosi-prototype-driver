package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PluginRegistrationSpec defines the desired state of PluginRegistration
type PluginRegistrationSpec struct {
	PluginName string `json:"pluginName"`
	Host       string `json:"host"`

	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// PluginRegistrationStatus defines the observed state of PluginRegistration
type PluginRegistrationStatus struct {
	CertSecret *corev1.SecretReference `json:"certSecret"`

	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PluginRegistration is the Schema for the pluginregistrations API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=pluginregistrations,scope=Namespaced
type PluginRegistration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PluginRegistrationSpec   `json:"spec,omitempty"`
	Status PluginRegistrationStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PluginRegistrationList contains a list of PluginRegistration
type PluginRegistrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PluginRegistration `json:"items"`
}
