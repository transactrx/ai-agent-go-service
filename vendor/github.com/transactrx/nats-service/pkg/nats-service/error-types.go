package nats_service

import "fmt"

type NatsServiceError struct {
	Status        int    `json:"status"` //	similar to http error codes
	ErrorMessage  string `json:"errorMessage"`
	ApiStatusCode int    `json:"apiStatusCode"` //	this is a status that the calling method will define
	InternalErr   string `json:"internalError"`
}

func NewValidationError(errorMessage string, apiStatusCode int, err error) NatsServiceError {
	return NatsServiceError{
		Status:        400,
		ErrorMessage:  errorMessage,
		ApiStatusCode: apiStatusCode,
		InternalErr:   fmt.Sprintf("%v", err),
	}
}

func NewServerError(errorMessage string, apiStatusCode int, err error) NatsServiceError {
	return NatsServiceError{
		Status:        500,
		ErrorMessage:  errorMessage,
		ApiStatusCode: apiStatusCode,
		InternalErr:   fmt.Sprintf("%v", err),
	}
}

func NewAuthorizationError(errorMessage string, apiStatusCode int, err error) NatsServiceError {
	return NatsServiceError{
		Status:        403,
		ErrorMessage:  errorMessage,
		ApiStatusCode: apiStatusCode,
		InternalErr:   fmt.Sprintf("%v", err),
	}
}

func NewEndpointNotFoundError(subject string) NatsServiceError {
	return NatsServiceError{
		Status:        404,
		ErrorMessage:  fmt.Sprintf("subject: %s not found", subject),
		ApiStatusCode: 404,
	}
}

func NewForwardedError(subject string) NatsServiceError {
	return NatsServiceError{
		Status:        302,
		ErrorMessage:  fmt.Sprintf("subject: %s not found", subject),
		ApiStatusCode: 404,
	}
}
