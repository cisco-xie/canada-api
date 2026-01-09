package com.example.demo.api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.HttpHeaders;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

@Service
public class LoginService {

    @Autowired
    private WebClient.Builder webClientBuilder;

    public Mono<String> simulateLogin(String username, String password, String code) {
        String loginUrl = "https://7410893256-am.tcr195uhyru.com/login";

        // 构建请求参数
        String params = "account=" + username + "&password=" + password + "&code=" + code;

        return webClientBuilder.baseUrl(loginUrl)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded")
                .defaultHeader(HttpHeaders.COOKIE, "visid_incap_3042898=M3qcw+ILSISvGap1LvJiwA+oTmcAAAAAQUIPAAAAAADH+pwU6Aak+93oy43G+3Ln; nlbi_3042898=hTWZBeCWBFRoL63okFTcAQAAAAC1RatlySm0DPITeb1GQvWh; incap_ses_1509_3042898=K6zFVHlWimXJpZ/smgvxFA+oTmcAAAAA9M5cOgcy+ZwplxPmtQRjnQ==; ssid1=3e91457b967da350118bd75e026ee660; random=9241; JSESSIONID=43B429EB2458357C2572599E1061C66F; 2a29530a2306=1d17d23f-50c1-45a1-a199-c1acf32865c8; b-user-id=5f7b230f-c05f-86ba-9111-7527ee9abbe9")
                .build()
                .post()
                .uri(UriComponentsBuilder.fromHttpUrl(loginUrl).build().toUri())
                .bodyValue(params)
                .retrieve()  // 发起请求
                .bodyToMono(String.class);  // 返回响应体
    }
}

