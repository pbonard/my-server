package service

import "errors"

var (
	ErrSignInWithGoogle         = errors.New("fail to google sing in")
	ErrSignInWithApple          = errors.New("fail to apple sign in")
	ErrCheckEmail               = errors.New("fail to check email usability")
	ErrSignUpWithEmail          = errors.New("fail to sign up with email")
	ErrLoginWithEmail           = errors.New("email or password is(are) incorrect")
	ErrSendEmailOTP             = errors.New("fail to send email OTP")
	ErrVerifyEmailOTP           = errors.New("fail to verify email OTP")
	ErrEmailOTPTooSoon          = errors.New("please wait a minute and try again")
	ErrUseThirdPartySignIn      = errors.New("please continue with Google or Apple")
	ErrVerifyTOTP               = errors.New("that code is incorrect")
	ErrTOTPTooManyAttempts      = errors.New("too many attempts, please wait a few minutes")
	ErrEmailSignedUpAgain       = errors.New("this email was signed up again, please sign up again")
	ErrSessionExpired           = errors.New("session expired, please verify again")
	ErrPhoneNumberAlreadyLinked = errors.New("this phone number already linked with other account")
	ErrSendSMSOTP               = errors.New("fail to send SMS OTP")
	ErrVerifySMSOTP             = errors.New("fail to verify SMS OTP")
	ErrGenerateToken            = errors.New("fail to generate new access token")
	ErrFailToSignOut            = errors.New("fail to sign out")

	ErrInternalServer = errors.New("something went wrong")
)
