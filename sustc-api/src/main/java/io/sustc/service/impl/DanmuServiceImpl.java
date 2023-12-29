package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.core.Local;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import javax.xml.transform.Result;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
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
            if (auth.getQq() != null && auth.getPassword() != null) {
                if (rs_check_password.getLong(1) != rs_checkqq_match.getLong(1)) {
                    System.out.println("qq and password don't match!");
                    return false;
                }
            }//提供了密码和qq，但是二者不匹配。
            if (auth.getWechat() != null && auth.getPassword() != null) {
                if (rs_check_wechat_match.getLong(1) != rs_check_password.getLong(1)) {
                    System.out.println("wechat and password don't match!");
                    return false;
                }
            }//提供了密码和微信，但是两者不匹配
            if (auth.getWechat() != null && auth.getQq() != null) {
                if (rs_check_qq_wechat_match.wasNull()) {
                    System.out.println("qq and wechat don't match!");
                    return false;
                }
            }//提供了微信和qq，但是两者不匹配
            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public Timestamp find_danmu_video(String bv) {
        String sql_find_danmu_video = "Select publictime from \"VideoRecord\" where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_danmu_video = conn.prepareStatement(sql_find_danmu_video);
        ) {
            stmt_find_danmu_video.setString(1, bv);
            log.info("SQL: {}", stmt_find_danmu_video);
            ResultSet rs_find_danmu_video = stmt_find_danmu_video.executeQuery();
            if (rs_find_danmu_video.wasNull()) {
                return null;
            } else {
                rs_find_danmu_video.next();
                return rs_find_danmu_video.getTimestamp(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean check_view(AuthInfo auth, String bv) {
        String sql_check_view = "Select * from view_video where bv = ? and view_mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_check_view = conn.prepareStatement(sql_check_view);
        ) {
            stmt_check_view.setString(1, bv);
            stmt_check_view.setLong(2, auth.getMid());
            log.info("SQL: {}", stmt_check_view);
            ResultSet rs_check_view = stmt_check_view.executeQuery();
            if (rs_check_view.wasNull()) {
                return false;
            } else return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long generate_danmu_id(AuthInfo auth) {
        long userId = auth.getMid();// 获取用户ID的方法，例如通过auth对象获取
        long timestamp = System.currentTimeMillis();
        return (userId << 32) | (timestamp & 0xFFFFFFFFL);
    }

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (!CheckAuthoInfo(auth)) {
            return -1;
        }
        LocalDateTime localDateTime = LocalDateTime.now();
        if (find_danmu_video(bv) == null || find_danmu_video(bv).after(Timestamp.valueOf(localDateTime))) {
            return -1;
        }
        if (content == null) {
            return -1;
        }
        if (!check_view(auth, bv)) {
            return -1;
        }
        //边界条件满足，需要更新danmu表
        String sql_insert_DanmuRecord = "INSERT INTO \"DanmuRecord\" (bv, mid, time, content, posttime, likedby, danmu_is_deleted) VALUES (?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_insert_DanmuRecord = conn.prepareStatement(sql_insert_DanmuRecord);
        ) {
            stmt_insert_DanmuRecord.setString(1, bv);
            stmt_insert_DanmuRecord.setLong(2, auth.getMid());
            stmt_insert_DanmuRecord.setFloat(3, time);
            stmt_insert_DanmuRecord.setString(4, content);
            stmt_insert_DanmuRecord.setTimestamp(5, Timestamp.valueOf(localDateTime));
            stmt_insert_DanmuRecord.setArray(6, null);
            stmt_insert_DanmuRecord.setBoolean(7, false);
            log.info("SQL: {}", stmt_insert_DanmuRecord);
            stmt_insert_DanmuRecord.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return generate_danmu_id(auth);
    }

    public float find_video_duration(String bv) {
        String sql_find_video_duration = "Select duration from \"VideoRecord\" where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_video_duration = conn.prepareStatement(sql_find_video_duration);
        ) {
            stmt_find_video_duration.setString(1, bv);
            log.info("SQL: {}", stmt_find_video_duration);
            ResultSet rs_find_video_duration = stmt_find_video_duration.executeQuery();
            if (rs_find_video_duration.wasNull()) {
                return -1;
            } else {
                rs_find_video_duration.next();
                return rs_find_video_duration.getFloat(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        LocalDateTime localDateTime = LocalDateTime.now();
        float duration = find_video_duration(bv);
        List<DanmuRecord> danmuRecords = new ArrayList<>();
        if (find_danmu_video(bv) == null || find_danmu_video(bv).after(Timestamp.valueOf(localDateTime))) {
            return null;
        }
        if (timeStart >= timeEnd || timeStart < 0 || timeEnd < 0 || timeStart > duration || timeEnd > duration) {
            return null;
        }
        String sql_display_danmu = "Select id,time,content, posttime from \"DanmuRecord\" where bv = ? and time >= ? and time <= ? order by posttime ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_display_danmu = conn.prepareStatement(sql_display_danmu);
        ) {
            stmt_display_danmu.setString(1, bv);
            stmt_display_danmu.setFloat(2, timeStart);//TODO:需要检查timeStart和timeEND
            stmt_display_danmu.setFloat(3, timeEnd);
            log.info("SQL: {}", stmt_display_danmu);
            ResultSet rs_display_danmu = stmt_display_danmu.executeQuery();
            if (rs_display_danmu == null) {
                return null;
            } else {
                while (rs_display_danmu.next()) {
                    DanmuRecord danmuRecord = new DanmuRecord();
                    danmuRecord.setId(rs_display_danmu.getLong(1));
                    danmuRecord.setTime(rs_display_danmu.getLong(2));
                    danmuRecord.setContent(rs_display_danmu.getString(3));
                    danmuRecord.setPostTime(rs_display_danmu.getTimestamp(4));
                    danmuRecords.add(danmuRecord);
                }
            }
            List<Long> finalList = new ArrayList<>();

            if (filter) {//打开过滤，只展示最早的内容相同的弹幕
                Timestamp earliest = danmuRecords.get(1).getPostTime();
                String content = danmuRecords.get(1).getContent();
                for (DanmuRecord danmuRecord : danmuRecords) {
                    if (danmuRecord.getPostTime().equals(earliest) && danmuRecord.getContent().equals(content)) {
                        finalList.add(danmuRecord.getId());
                    } else {
                        break;
                    }
                }
                return finalList;
            } else {
                for (DanmuRecord danmuRecord : danmuRecords) {
                    finalList.add(danmuRecord.getId());
                }
                return finalList;
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public String find_danmu_bv(long id) {
        String sql_find_danmu_bv = "Select bv from \"DanmuRecord\" where id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_find_danmu_bv = conn.prepareStatement(sql_find_danmu_bv);
        ) {
            stmt_find_danmu_bv.setLong(1, id);
            log.info("SQL: {}", stmt_find_danmu_bv);
            ResultSet rs_find_danmu_bv = stmt_find_danmu_bv.executeQuery();
            if (rs_find_danmu_bv.wasNull()) {
                return null;
            } else {
                rs_find_danmu_bv.next();
                return rs_find_danmu_bv.getString(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!CheckAuthoInfo(auth)) {
            return false;
        }
        //需要观看之后才能发弹幕
        String bv = find_danmu_bv(id);
        if (!check_view(auth, bv)) {
            return false;
        }
        String sql_danmu = "Select * from \"DanmuRecord\" where id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_danmu = conn.prepareStatement(sql_danmu);
        ) {
            stmt_danmu.setLong(1, id);
            log.info("SQL: {}", stmt_danmu);
            ResultSet rs_danmu = stmt_danmu.executeQuery();
            if (rs_danmu.wasNull()) {
                return false;
            } else {
                rs_danmu.next();
                String sql_danmu_like = "Select * from danmu_like where danmu_like_mid = ? and id = ?";
                try (Connection conn_1 = dataSource.getConnection();
                     PreparedStatement stmt_danmu_like = conn_1.prepareStatement(sql_danmu_like);
                ) {
                    stmt_danmu_like.setLong(1, auth.getMid());
                    stmt_danmu_like.setLong(2, id);
                    log.info("SQL: {}", stmt_danmu_like);
                    ResultSet rs_danmu_like = stmt_danmu_like.executeQuery();
                    if (rs_danmu_like.wasNull()) {//没有给该弹幕点过赞
                        String sql_danmu_like_insert = "UPDATE \"DanmuRecord\" set likedby = array_cat(likedby,Array[?]) where id = ?";
                        String sql_danmu_insert = "INSERT INTO danmu_like (bv, danmu_like_mid, id) VALUES (?,?,?)";
                        try (Connection conn_2 = dataSource.getConnection();
                             PreparedStatement stmt_danmu_like_insret = conn_2.prepareStatement(sql_danmu_like_insert);
                             PreparedStatement stmt_danmu_insert = conn_2.prepareStatement(sql_danmu_insert);
                        ) {
                            stmt_danmu_like_insret.setLong(1, auth.getMid());
                            stmt_danmu_like_insret.setLong(2, id);
                            log.info("SQL: {}", stmt_danmu_like_insret);
                            stmt_danmu_like_insret.executeUpdate();
                            stmt_danmu_insert.setString(1, find_danmu_bv(id));
                            stmt_danmu_insert.setLong(2, auth.getMid());
                            stmt_danmu_insert.setLong(3, id);
                            log.info("SQL: {}", stmt_danmu_insert);
                            stmt_danmu_insert.executeUpdate();
                            return true;
                        }
                    } else {//已经给弹幕点过赞了，再次点击取消点赞
                        String sql_delete_like = "UPDATE \"DanmuRecord\" set likedby = array_remove(likedby,Array[?]) where id = ?";
                        String sql_delete = "DELETE from danmu_like where danmu_like_mid = ? and id = ?";
                        try (PreparedStatement stmt_delete_like = conn.prepareStatement(sql_delete_like);
                             PreparedStatement stmt_delete = conn.prepareStatement(sql_delete);
                        ) {
                            stmt_delete_like.setLong(1, auth.getMid());
                            stmt_delete_like.setLong(2, id);
                            log.info("SQL: {}", stmt_delete_like);
                            stmt_delete_like.executeUpdate();
                            stmt_delete.setLong(1, auth.getMid());
                            stmt_delete.setLong(2, id);
                            log.info("SQL: {}", stmt_delete);
                            stmt_delete.executeUpdate();
                            return false;
                        }
                    }

                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
