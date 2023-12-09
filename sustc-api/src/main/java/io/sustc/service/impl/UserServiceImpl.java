package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.dto.RegisterUserReq;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;
    private List<UserRecord> users;

    @Override
    public long register(RegisterUserReq req) {
        if (req.getPassword() == null || req.getName() == null || req.getSex() == null) {
            return -1;
        }
        if (req.getBirthday() != null && !req.getBirthday().matches("\\d{1,2}月\\d{1,2}日")) {
            return -1;
        }
//        String sql = "Select count(*) from \"UserRecord\" where Name = ? or qq = ? or Wechat = ?";
//        //考虑两种方法：1. 在UserRecord类中添加一个静态的表，每次添加一个用户后就把信息存到表中； 2. 直接连接数据库进行比较
//        try (Connection connection = dataSource.getConnection();
//             PreparedStatement statement = connection.prepareStatement(sql);
//        ) {
//            statement.setString(1, req.getName());
//            statement.setString(2, req.getQq());
//            statement.setString(3, req.getWechat());
//
//            ResultSet resultSet = statement.executeQuery();
//            resultSet.next();
//            if(resultSet.getInt(1)>0){
//                return -1;
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        for (UserRecord existingUser : users) {
            if (existingUser.getName().equals(req.getName()) ||
                    (req.getQq() != null && !req.getQq().isEmpty() && existingUser.getQq() != null && existingUser.getQq().equals(req.getQq())) ||
                    (req.getWechat() != null && !req.getWechat().isEmpty() && existingUser.getWechat() != null && existingUser.getWechat().equals(req.getWechat()))) {
                return -1;
            }
        }
        UserRecord newUser = new UserRecord();
        Long mid = generatedNewMid();
        newUser.setMid(mid);
        newUser.setName(req.getName());
        newUser.setSex(req.getSex().name());
        if (req.getBirthday() != null) {
            newUser.setBirthday(req.getBirthday());
        }
        newUser.setLevel((short) 0); // Assuming default level is 0
        newUser.setCoin(0); // Assuming default coin is 0
        if (req.getSign() != null) {
            newUser.setSign(req.getSign());
        }
//        newUser.setIdentity(UserRecord.Identity.USER); // Assuming default identity is USER
        newUser.setPassword(req.getPassword());
        if (req.getQq() != null && req.getWechat() != null) {
            newUser.setQq(req.getQq());
            newUser.setWechat(req.getWechat());
        }

        // Add the new user to the list
        users.add(newUser);
        return mid;
    }

    private long generatedNewMid() {
        return users.size() + 1;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        return null;
    }
}
