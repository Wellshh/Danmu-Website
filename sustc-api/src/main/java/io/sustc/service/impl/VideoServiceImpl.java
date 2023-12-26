package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;

    public boolean CheckAuthoInfo(AuthInfo auth) {
        String sql_mid = "Select Password from \"UserRecord\" where mid = ?";//密码是否匹配
        String sql_qq_wechat = "SELECT ((SELECT mid FROM \"UserRecord\" WHERE qq = ?) INTERSECT (SELECT mid FROM \"UserRecord\" WHERE wechat = ?))";//如果同时提供了qq和微信，是否能够匹配。
        String sql_qq = "Select mid from \"UserRecord\" where QQ = ?";
        String sql_wechat = "Select mid from \"UserRecord\" where wechat =?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_check_password = conn.prepareStatement(sql_mid);
             PreparedStatement stmt_check_qq_wechat_match = conn.prepareStatement(sql_qq_wechat);
             PreparedStatement stmt_checkqq_match = conn.prepareStatement(sql_qq);
             PreparedStatement stmt_check_wechat_match = conn.prepareStatement(sql_wechat);

        ) {
            stmt_check_password.setLong(1, auth.getMid());
            log.info("SQL: {}", stmt_check_password);
            stmt_check_qq_wechat_match.setString(1, auth.getQq());
            stmt_check_qq_wechat_match.setString(2, auth.getWechat());
            log.info("SQL: {}", stmt_check_qq_wechat_match);
            stmt_checkqq_match.setString(1, auth.getQq());
            log.info("SQL: {}", stmt_checkqq_match);
            stmt_check_wechat_match.setString(1, auth.getWechat());
            log.info("SQL: {}", stmt_check_wechat_match);
            ResultSet rs_check_password = stmt_check_password.executeQuery();
            rs_check_password.next();
            ResultSet rs_check_qq_wechat_match = stmt_check_qq_wechat_match.executeQuery();
            rs_check_qq_wechat_match.next();
            ResultSet rs_checkqq_match = stmt_checkqq_match.executeQuery();
//            rs_check_qq_wechat_match.next();
            ResultSet rs_check_wechat_match = stmt_check_wechat_match.executeQuery();
            rs_check_wechat_match.next();
            //如果没有提供任何登陆方式或者只提供了用户名或者密码，返回false
            if (auth.getQq() == null && auth.getWechat() == null && (auth.getMid() == 0 || auth.getPassword() == null)) {
                System.out.println("No log in information is provided!");
            }
            //只提供了qq或者微信，登陆成功
            if (auth.getMid() == 0 && auth.getPassword() == null && ((auth.getQq() != null && auth.getWechat() == null) || (auth.getQq() == null && auth.getWechat() != null))) {
                if (auth.getQq() != null) {
                    if (!rs_checkqq_match.wasNull()) {
                        System.out.println("Only qq is provided!");
                        return true;
                    }
                } else if (auth.getWechat() != null) {
                    if (!rs_check_wechat_match.wasNull()) {
                        System.out.println("Only wechat is provided!");
                        return true;
                    }
                }
            }
            //提供了两项信息，两个信息是同一个人的信息，可以登陆
            if (auth.getPassword() != null) {
                if (rs_check_password.getLong(1) != auth.getMid()) {
                    System.out.println("Mid and password don't match!");
                    return false;
                }
            }//密码与人不匹配
            if(auth.getQq() != null && auth.getPassword() != null){
                if(rs_check_password.getLong(1)!= rs_checkqq_match.getLong(1)){
                    System.out.println("qq and password don't match!");
                    return false;
                }
            }//提供了密码和qq，但是二者不匹配。
            if(auth.getWechat() != null && auth.getPassword() != null){
                if(rs_check_wechat_match.getLong(1)!= rs_check_password.getLong(1)) {
                    System.out.println("wechat and password don't match!");
                    return false;
                }
            }//提供了密码和微信，但是两者不匹配
            if(auth.getWechat() != null && auth.getQq() != null){
                if(rs_check_qq_wechat_match.wasNull()){
                    System.out.println("qq and wechat don't match!");
                    return false;
                }
            }//提供了微信和qq，但是两者不匹配
            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    //如何生成独一无二的bv号？
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        String sql_sametitle_and_user = "SELECT COUNT(*) FROM \"VideoRecord\" v JOIN \"UserRecord\" u ON v.OwnerMid = u.mid WHERE v.Title = ? AND v.OwnerMid = ?";
        String sql_findmidname = "SELECT u.name FROM \"UserRecord\" u WHERE u.mid = ?";
        VideoRecord newvideoRecord = new VideoRecord();
        if (!CheckAuthoInfo(auth)) {
            System.out.println("Authinfo is wrong!");
            return null;
        } else {
            if (req.getTitle() == null) {
                System.out.println("title is null!");
                return null;
            }
            if (req.getDuration() < 10) {
                System.out.println("duration is too short!");
                return null;
            }
            LocalDateTime localDateTime = LocalDateTime.now();
            if (req.getPublicTime().before(Timestamp.valueOf(localDateTime))) {
                System.out.println("Public time is too early!");
                return null;
            }
            String sql_insertvideo = "Insert into \"VideoRecord\" (bv, title, ownermid, ownername, committime, duration, description)\n" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (Connection conn = dataSource.getConnection();
                 Connection conn_insertvideo = dataSource.getConnection();
                 PreparedStatement stmt_sametitle_and_user = conn.prepareStatement(sql_sametitle_and_user);
                 PreparedStatement stmt_findmidname = conn.prepareStatement(sql_findmidname);
                 PreparedStatement stmt_insertuser = conn_insertvideo.prepareStatement(sql_insertvideo);

            ) {
                stmt_sametitle_and_user.setString(1, req.getTitle());
                stmt_sametitle_and_user.setLong(2, auth.getMid());
                log.info("SQL: {}", stmt_sametitle_and_user);
                ResultSet rs_sametitle_and_user = stmt_sametitle_and_user.executeQuery();
                rs_sametitle_and_user.next();
                if (rs_sametitle_and_user.getInt(1) != 0) {
                    System.out.println("There is already a same video!");
                    return null;
                }
                stmt_findmidname.setLong(1, auth.getMid());
                log.info("SQL: {}", stmt_findmidname);
                ResultSet rs_findmidname = stmt_findmidname.executeQuery();
                rs_findmidname.next();
                String authname = rs_findmidname.getString(1);
                newvideoRecord.setBv(generatebv());
                newvideoRecord.setTitle(req.getTitle());
                newvideoRecord.setOwnerMid(auth.getMid());
                newvideoRecord.setOwnerName(authname);
                newvideoRecord.setCommitTime(Timestamp.valueOf(localDateTime));
                newvideoRecord.setDuration(req.getDuration());
                newvideoRecord.setDescription(req.getDescription());
                newvideoRecord.setVideo_is_Deleted(false);
                stmt_insertuser.setString(1, newvideoRecord.getBv());
                stmt_insertuser.setString(2, newvideoRecord.getTitle());
                stmt_sametitle_and_user.setLong(3, newvideoRecord.getOwnerMid());
                stmt_sametitle_and_user.setString(4, newvideoRecord.getOwnerName());
                stmt_sametitle_and_user.setTimestamp(5, newvideoRecord.getCommitTime());
                stmt_sametitle_and_user.setFloat(6, newvideoRecord.getDuration());
                stmt_sametitle_and_user.setString(7, newvideoRecord.getDescription());
                log.info("SQL: {}", stmt_insertuser);
                stmt_sametitle_and_user.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return newvideoRecord.getBv();
    }

    public String generatebv() {
        return "BV2".concat(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        String sql_user_identiey = "Select identity from \"UserRecord\" where mid = ?";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt_user_identiey = conn.prepareStatement(sql_user_identiey);
        ){} catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
