package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The authorization information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
//身份验证类，使用这个类的实例，你可以创建一个包含身份验证信息的对象，并将其传递给相关的身份验证功能或服务。这样的类在处理身份验证时，可以更清晰地传递和维护不同身份验证方式的信息。
public class AuthInfo implements Serializable {

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;
}
