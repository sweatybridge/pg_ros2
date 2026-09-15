fn main() {
    println!("cargo:rerun-if-env-changed=ROS_DISTRO");
    assert_eq!(
        std::env::var("ROS_DISTRO").as_deref(),
        Ok("humble"),
        "pg_ros2 targets ROS 2 Humble: source /opt/ros/humble/setup.bash before building"
    );
}
